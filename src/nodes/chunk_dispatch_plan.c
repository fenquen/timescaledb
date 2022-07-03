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
#include <nodes/readfuncs.h>
#include <parser/parsetree.h>
#include <utils/rel.h>
#include <catalog/pg_type.h>
#include <rewrite/rewriteManip.h>

#include "chunk_dispatch_plan.h"
#include "chunk_dispatch_state.h"
#include "hypertable.h"

#include "compat/compat.h"

/*
 * Create a ChunkDispatchState node from this plan. This is the full execution
 * state that replaces the plan node as the plan moves from planning to execution.
 */
static Node *create_chunk_dispatch_state(CustomScan *customScan) {
	return (Node *) ts_chunk_dispatch_state_create(linitial_oid(customScan->custom_private),
												   linitial(customScan->custom_plans));
}

static CustomScanMethods chunk_dispatch_plan_methods = {
	.CustomName = "ChunkDispatch",
	.CreateCustomScanState = create_chunk_dispatch_state,
};

/* Create a chunk dispatch plan node in the form of a CustomScan node. The
 * purpose of this plan node is to dispatch (route) tuples to the correct chunk
 * in a hypertable.
 *
 * Note that CustomScan nodes cannot be extended (by struct embedding) because
 * they might be copied, therefore we pass hypertable_relid in the custom_private field.
 *
 * The chunk dispatch plan takes the original tuple-producing subplan, which
 * was part of a ModifyTable node, and imposes itself between the
 * ModifyTable plan and the subplan. During execution, the subplan will
 * produce the new tuples that the chunk dispatch node routes before passing
 * them up to the ModifyTable node.
 */
static Plan *chunk_dispatch_plan_create(PlannerInfo *root,
										RelOptInfo *relopt,
										CustomPath *best_path,
										List *tlist,
										List *clauses,
										List *custom_plans) {
	ChunkDispatchPath *chunkDispatchPath = (ChunkDispatchPath *) best_path;
	CustomScan *customScan = makeNode(CustomScan);
	ListCell *lc;

	foreach (lc, custom_plans) {
		Plan *subplan = lfirst(lc);
		customScan->scan.plan.startup_cost += subplan->startup_cost;
		customScan->scan.plan.total_cost += subplan->total_cost;
		customScan->scan.plan.plan_rows += subplan->plan_rows;
		customScan->scan.plan.plan_width += subplan->plan_width;
	}

	customScan->custom_private = list_make1_oid(chunkDispatchPath->hypertable_relid);
	customScan->methods = &chunk_dispatch_plan_methods;
	customScan->custom_plans = custom_plans;
	customScan->scan.scanrelid = 0; /* Indicate this is not a real relation we are scanning */

	/* The "input" and "output" target lists should be the same */
	customScan->custom_scan_tlist = tlist;
	customScan->scan.plan.targetlist = tlist;

	return &customScan->scan.plan;
}

static CustomPathMethods chunk_dispatch_path_methods = {
	.CustomName = "ChunkDispatchPath",
	// 该函数注入chunk_dispatch_plan_methods
	.PlanCustomPath = chunk_dispatch_plan_create,
};

Path *ts_chunk_dispatch_path_create(PlannerInfo *root,
									ModifyTablePath *modifyTablePath,
									Index hypertable_rti,
									int subpath_index) {
	ChunkDispatchPath *chunkDispatchPath = (ChunkDispatchPath *) palloc0(sizeof(ChunkDispatchPath));

#if PG14_LT
	Path *subPath = list_nth(modifyTablePath->subpaths, subpath_index); // chunkDispatch
#else
	Path *subPath = modifyTablePath->subPath;
#endif

	RangeTblEntry *rangeTableEntry = planner_rt_fetch(hypertable_rti, root); // 对应目标表

	memcpy(&chunkDispatchPath->cpath.path, subPath, sizeof(Path));
	chunkDispatchPath->cpath.path.type = T_CustomPath;
	chunkDispatchPath->cpath.path.pathtype = T_CustomScan;

	chunkDispatchPath->cpath.methods = &chunk_dispatch_path_methods;
	chunkDispatchPath->cpath.custom_paths = list_make1(subPath);

	chunkDispatchPath->mtpath = modifyTablePath;
	chunkDispatchPath->hypertable_rti = hypertable_rti;
	chunkDispatchPath->hypertable_relid = rangeTableEntry->relid;

	return &chunkDispatchPath->cpath.path;
}
