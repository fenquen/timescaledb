/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <catalog/pg_class.h>
#include <commands/trigger.h>
#include <nodes/nodes.h>
#include <nodes/extensible.h>
#include <executor/nodeModifyTable.h>

#include "compat/compat.h"
#include "chunk_dispatch_state.h"
#include "chunk_dispatch_plan.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "chunk.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "dimension.h"
#include "hypertable.h"

static void chunk_dispatch_begin(CustomScanState *customScanState,
								 EState *estate,
								 int eflags) {
	ChunkDispatchState *chunkDispatchState = (ChunkDispatchState *) customScanState;

	Cache *hypertable_cache;
	Hypertable *hyperTable = ts_hypertable_cache_get_cache_and_entry(chunkDispatchState->hypertable_relid,
																	 CACHE_FLAG_NONE,
																	 &hypertable_cache);

	PlanState *planState = ExecInitNode(chunkDispatchState->subplan, estate, eflags); // resultState
	chunkDispatchState->hypertable_cache = hypertable_cache;
	chunkDispatchState->dispatch = ts_chunk_dispatch_create(hyperTable, estate, eflags);
	chunkDispatchState->dispatch->dispatch_state = chunkDispatchState;

	customScanState->custom_ps = list_make1(planState);
}

/*
 * Change to another chunk for inserts.
 * Prepare the ModifyTableState executor node for inserting into another
 * chunk. Called every time we switch to another chunk for inserts.
 */
static void on_chunk_insert_state_changed(ChunkInsertState *chunkInsertState, void *data) { // chunkDispatchState
	ChunkDispatchState *chunkDispatchState = data;
#if PG14_LT
	ModifyTableState *modifyTableState = chunkDispatchState->mtstate;

	/* PG < 14 expects the current target slot to match the result relation.
	 * we need to make sure it is up-to-date with the current chunk here. */
	modifyTableState->mt_scans[modifyTableState->mt_whichplan] = chunkInsertState->slot;
#endif

	// 这个属性只用在了data node
	chunkDispatchState->rri = chunkInsertState->result_relation_info;
}

static TupleTableSlot *chunk_dispatch_exec(CustomScanState *customScanState) {
	ChunkDispatchState *chunkDispatchState = (ChunkDispatchState *) customScanState;
	PlanState *childPlanState = linitial(customScanState->custom_ps); // chunk_dispatch_begin 注入 customScanState->custom_ps = list_make1(planState);

	ChunkDispatch *chunkDispatch = chunkDispatchState->dispatch;
	Hypertable *hypertable = chunkDispatch->hypertable;
	EState *estate = customScanState->ss.ps.state;

	/* get the next tuple from the sub plan chunkDispatchState customScanState */
	// 到了这里便是执行了最小弟的那个了 原来的insert小弟 对应 insert 中的 要insert的数据 便是单行的data
	TupleTableSlot *tupleTableSlot = ExecProcNode(childPlanState);

	if (TupIsNull(tupleTableSlot)) {
		return NULL;
	}

	/* Reset the per-tuple exprcontext */
	ResetPerTupleExprContext(estate);

	/* Switch to the executor's per-tuple memory context */
	MemoryContext old = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	/* Calculate the tuple's point in the N-dimensional hyperspace */
	Point *point = ts_hyperspace_calculate_point(hypertable->space, tupleTableSlot);

	/* save the main table's (hypertable's) ResultRelInfo */
	if (!chunkDispatch->hypertable_result_rel_info) {
#if PG14_LT
		Assert(RelationGetRelid(estate->es_result_relation_info->ri_RelationDesc) == chunkDispatchState->hypertable_relid);
		chunkDispatch->hypertable_result_rel_info = estate->es_result_relation_info;
#else
		chunkDispatch->hypertable_result_rel_info = chunkDispatch->dispatch_state->mtstate->resultRelInfo;
#endif
	}

	/* Find or create the insert chunkDispatchState matching the point */
	// 虽然 chunkInsertState没有return 在该函数的内部给它找了
	// chunkDispatch->prev_cis = chunkInsertState;
	// chunkDispatch->prev_cis_oid = chunkInsertState->rel->rd_id;
	ChunkInsertState *chunkInsertState = ts_chunk_dispatch_get_chunk_insert_state(chunkDispatch,
																				  point,
																				  on_chunk_insert_state_changed,
																				  chunkDispatchState);

	/*
	 * Set the result relation in the executor chunkDispatchState to the target chunk.
	 * This makes sure that the tuple gets inserted into the correct
	 * chunk. Note that since in PG < 14 the ModifyTable executor saves and restores
	 * the es_result_relation_info this has to be updated every time, not
	 * just when the chunk changes.
	 */
#if PG14_LT
	if (chunkInsertState->compress_info != NULL) {
		estate->es_result_relation_info = chunkInsertState->compress_info->orig_result_relation_info;
	} else {
		// chunkInsertState的ResultRelInfo转移到了modifyTable
		estate->es_result_relation_info = chunkInsertState->result_relation_info;
	}
#endif

	MemoryContextSwitchTo(old);

	/* Convert the tuple to the chunk's row type, if necessary */
	// 把原来的要insert的值迁移
	if (chunkInsertState->hyper_to_chunk_map != NULL) {
		tupleTableSlot = execute_attr_map_slot(chunkInsertState->hyper_to_chunk_map->attrMap,
											   tupleTableSlot,
											   chunkInsertState->slot);
	}

	if (chunkInsertState->compress_info != NULL) {
		/*
		 * When the chunk is compressed, we redirect the insert to the internal compressed
		 * chunk. However, any BEFORE ROW triggers defined on the chunk have to be executed
		 * before we redirect the insert.
		 */
		if (chunkInsertState->compress_info->orig_result_relation_info->ri_TrigDesc &&
			chunkInsertState->compress_info->orig_result_relation_info->ri_TrigDesc->trig_insert_before_row) {
			bool skip_tuple;
			skip_tuple =
				!ExecBRInsertTriggers(estate, chunkInsertState->compress_info->orig_result_relation_info, tupleTableSlot);

			if (skip_tuple)
				return NULL;
		}

		if (chunkInsertState->rel->rd_att->constr && chunkInsertState->rel->rd_att->constr->has_generated_stored)
			ExecComputeStoredGeneratedCompat(chunkInsertState->compress_info->orig_result_relation_info,
											 estate,
											 tupleTableSlot,
											 CMD_INSERT);

		if (chunkInsertState->rel->rd_att->constr)
			ExecConstraints(chunkInsertState->compress_info->orig_result_relation_info, tupleTableSlot, estate);

#if PG14_LT
		estate->es_result_relation_info = chunkInsertState->result_relation_info;
#endif
		Assert(ts_cm_functions->compress_row_exec != NULL);
		TupleTableSlot *orig_slot = tupleTableSlot;
		tupleTableSlot = ts_cm_functions->compress_row_exec(chunkInsertState->compress_info->compress_state, tupleTableSlot);
		/* If we have cagg defined on the hypertable, we have to execute
		 * the function that records invalidations directly as AFTER ROW
		 * triggers do not work with compressed chunks.
		 */
		if (chunkInsertState->compress_info->has_cagg_trigger) {
			Assert(ts_cm_functions->continuous_agg_call_invalidation_trigger);
			HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) orig_slot;
			if (!hslot->tuple)
				hslot->tuple = heap_form_tuple(orig_slot->tts_tupleDescriptor,
											   orig_slot->tts_values,
											   orig_slot->tts_isnull);

			ts_compress_chunk_invoke_cagg_trigger(chunkInsertState->compress_info, chunkInsertState->rel, hslot->tuple);
		}
	}

	return tupleTableSlot;
}

static void
chunk_dispatch_end(CustomScanState *node) {
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	PlanState *substate = linitial(node->custom_ps);

	ExecEndNode(substate);
	ts_chunk_dispatch_destroy(state->dispatch);
	ts_cache_release(state->hypertable_cache);
}

static void
chunk_dispatch_rescan(CustomScanState *node) {
	PlanState *substate = linitial(node->custom_ps);

	ExecReScan(substate);
}

static CustomExecMethods chunk_dispatch_state_methods = {
	.CustomName = "ChunkDispatchState",
	.BeginCustomScan = chunk_dispatch_begin,
	.EndCustomScan = chunk_dispatch_end,
	.ExecCustomScan = chunk_dispatch_exec,
	.ReScanCustomScan = chunk_dispatch_rescan,
};

/*
 * Check whether the PlanState is a ChunkDispatchState node.
 */
bool ts_is_chunk_dispatch_state(PlanState *state) {
	CustomScanState *csstate = (CustomScanState *) state;

	if (!IsA(state, CustomScanState))
		return false;

	return csstate->methods == &chunk_dispatch_state_methods;
}

ChunkDispatchState *ts_chunk_dispatch_state_create(Oid originalTableOid,
												   Plan *subPlan) {
	ChunkDispatchState *chunkDispatchState = (ChunkDispatchState *) newNode(sizeof(ChunkDispatchState), T_CustomScanState);
	chunkDispatchState->hypertable_relid = originalTableOid;
	chunkDispatchState->subplan = subPlan;
	// 篡改原有的逻辑 cscan_state是pg的
	chunkDispatchState->cscan_state.methods = &chunk_dispatch_state_methods;
	return chunkDispatchState;
}

/*
 * This function is called during the init phase of the INSERT (ModifyTable)
 * plan, and gives the ChunkDispatchState node the access it needs to the
 * internals of the ModifyTableState node.
 *
 * Note that the function is called by the parent of the ModifyTableState node,
 * which guarantees that the ModifyTableState is fully initialized even though
 * ChunkDispatchState is a child of ModifyTableState.
 */
void ts_chunk_dispatch_state_set_parent(ChunkDispatchState *state, ModifyTableState *mtstate) {
	ModifyTable *mt_plan = castNode(ModifyTable, mtstate->ps.plan);

	/* Inserts on hypertables should always have one subplan */
#if PG14_LT
	Assert(mtstate->mt_nplans == 1);
#endif
	state->mtstate = mtstate;
	state->arbiter_indexes = mt_plan->arbiterIndexes;
}
