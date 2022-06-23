/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/nodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <utils/rel.h>
#include <catalog/pg_type.h>

#include "compat/compat.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "subspace_store.h"
#include "dimension.h"
#include "guc.h"

ChunkDispatch *
ts_chunk_dispatch_create(Hypertable *ht, EState *estate, int eflags) {
	ChunkDispatch *cd = palloc0(sizeof(ChunkDispatch));

	cd->hypertable = ht;
	cd->estate = estate;
	cd->eflags = eflags;
	cd->hypertable_result_rel_info = NULL;
	cd->cache =
		ts_subspace_store_init(ht->space, estate->es_query_cxt, ts_guc_max_open_chunks_per_insert);
	cd->prev_cis = NULL;
	cd->prev_cis_oid = InvalidOid;

	return cd;
}

static inline ModifyTableState *
get_modifytable_state(const ChunkDispatch *dispatch) {
	return dispatch->dispatch_state->mtstate;
}

static inline ModifyTable *
get_modifytable(const ChunkDispatch *dispatch) {
	return castNode(ModifyTable, get_modifytable_state(dispatch)->ps.plan);
}

bool ts_chunk_dispatch_has_returning(const ChunkDispatch *dispatch) {
	if (!dispatch->dispatch_state)
		return false;
	return get_modifytable(dispatch)->returningLists != NIL;
}

List *
ts_chunk_dispatch_get_returning_clauses(const ChunkDispatch *dispatch) {
#if PG14_LT
	ModifyTableState *mtstate = get_modifytable_state(dispatch);
	return list_nth(get_modifytable(dispatch)->returningLists, mtstate->mt_whichplan);
#else
	Assert(list_length(get_modifytable(dispatch)->returningLists) == 1);
	return linitial(get_modifytable(dispatch)->returningLists);
#endif
}

List *
ts_chunk_dispatch_get_arbiter_indexes(const ChunkDispatch *dispatch) {
	return dispatch->dispatch_state->arbiter_indexes;
}

OnConflictAction ts_chunk_dispatch_get_on_conflict_action(const ChunkDispatch *chunkDispatch) {
	if (!chunkDispatch->dispatch_state) {
		return ONCONFLICT_NONE;
	}

	return get_modifytable(chunkDispatch)->onConflictAction;
}

List *
ts_chunk_dispatch_get_on_conflict_set(const ChunkDispatch *dispatch) {
	return get_modifytable(dispatch)->onConflictSet;
}

CmdType ts_chunk_dispatch_get_cmd_type(const ChunkDispatch *dispatch) {
	return dispatch->dispatch_state == NULL ? CMD_INSERT :
											  dispatch->dispatch_state->mtstate->operation;
}

void ts_chunk_dispatch_destroy(ChunkDispatch *cd) {
	ts_subspace_store_free(cd->cache);
}

static void destroy_chunk_insert_state(void *chunkDispatch) {
	ts_chunk_insert_state_destroy((ChunkInsertState *) chunkDispatch);
}

// Get the chunk insert state for the chunk that matches the given point in the partitioned hyperspace.
extern ChunkInsertState *ts_chunk_dispatch_get_chunk_insert_state(ChunkDispatch *chunkDispatch,
																  Point *point,
																  const on_chunk_changed_func on_chunk_changed,
																  void *data) {
	bool chunkInsertStateChanged = true;

	/* Direct inserts into internal compressed hypertable is not supported.
	 * For compression chunks are created explicitly by compress_chunk and
	 * inserted into directly so we should never end up in this code path
	 * for a compressed hypertable.
	 */
	if (chunkDispatch->hypertable->fd.compression_state == HypertableInternalCompressionTable) {
		elog(ERROR, "direct insert into internal compressed hypertable is not supported");
	}




	// 试图得到对应的chunkInsertState
	ChunkInsertState *chunkInsertState = ts_subspace_store_get(chunkDispatch->cache, point);

	// 没有对应的
	if (NULL == chunkInsertState) {
		// 试图来get or create
		Chunk *newChunk = ts_hypertable_get_or_create_chunk(chunkDispatch->hypertable, point);

		if (NULL == newChunk) {
			elog(ERROR, "no chunk found or created");
		}

		// 纯新的创建
		chunkInsertState = ts_chunk_insert_state_create(newChunk, chunkDispatch);


		// 把这个新生成的添加到对应的链路的尾部
		ts_subspace_store_add(chunkDispatch->cache,
							  newChunk->cube,
							  chunkInsertState,
							  destroy_chunk_insert_state);
	} else if (chunkInsertState->rel->rd_id == chunkDispatch->prev_cis_oid && chunkInsertState == chunkDispatch->prev_cis) {
		/* got the same item from cache as before */
		chunkInsertStateChanged = false;
	}

	if (chunkInsertStateChanged && on_chunk_changed) {
		on_chunk_changed(chunkInsertState, data);
	}

	Assert(chunkInsertState != NULL);

	chunkDispatch->prev_cis = chunkInsertState;
	chunkDispatch->prev_cis_oid = chunkInsertState->rel->rd_id;

	return chunkInsertState;
}
