/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <access/htup_details.h>
#include <executor/tuptable.h>
#include <storage/lmgr.h>
#include <storage/bufmgr.h>
#include <storage/procarray.h>
#include <utils/rel.h>
#include <utils/palloc.h>
#include <utils/snapmgr.h>

#include "scanner.h"

enum ScannerType {
	ScannerTypeTable,
	ScannerTypeIndex,
};

/*
 * Scanner can implement both index and heap scans in a single interface.
 */
typedef struct Scanner {
	Relation (*openscan)(ScannerCtx *ctx);
	ScanDesc (*beginscan)(ScannerCtx *ctx);
	bool (*getnext)(ScannerCtx *ctx);
	void (*rescan)(ScannerCtx *ctx);
	void (*endscan)(ScannerCtx *ctx);
	void (*closescan)(ScannerCtx *ctx);
} Scanner;

/* Functions implementing heap scans */
static Relation
table_scanner_open(ScannerCtx *ctx) {
	ctx->tablerel = table_open(ctx->table, ctx->lockmode);
	return ctx->tablerel;
}

static ScanDesc
table_scanner_beginscan(ScannerCtx *ctx) {
	ctx->internal.scan.table_scan =
		table_beginscan(ctx->tablerel, ctx->snapshot, ctx->nkeys, ctx->scankey);

	return ctx->internal.scan;
}

static bool
table_scanner_getnext(ScannerCtx *ctx) {
	bool success = table_scan_getnextslot(ctx->internal.scan.table_scan,
										  ForwardScanDirection,
										  ctx->internal.tinfo.slot);

	return success;
}

static void
table_scanner_rescan(ScannerCtx *ctx) {
	table_rescan(ctx->internal.scan.table_scan, ctx->scankey);
}

static void
table_scanner_endscan(ScannerCtx *ctx) {
	table_endscan(ctx->internal.scan.table_scan);
}

static void
table_scanner_close(ScannerCtx *ctx) {
	LOCKMODE lockmode = (ctx->flags & SCANNER_F_KEEPLOCK) ? NoLock : ctx->lockmode;

	table_close(ctx->tablerel, lockmode);
}

/* Functions implementing index scans */
static Relation
index_scanner_open(ScannerCtx *ctx) {
	ctx->tablerel = table_open(ctx->table, ctx->lockmode);
	ctx->indexrel = index_open(ctx->index, ctx->lockmode);
	return ctx->indexrel;
}

static ScanDesc
index_scanner_beginscan(ScannerCtx *ctx) {
	InternalScannerCtx *ictx = &ctx->internal;

	ictx->scan.index_scan =
		index_beginscan(ctx->tablerel, ctx->indexrel, ctx->snapshot, ctx->nkeys, ctx->norderbys);
	ictx->scan.index_scan->xs_want_itup = ctx->want_itup;
	index_rescan(ictx->scan.index_scan, ctx->scankey, ctx->nkeys, NULL, ctx->norderbys);
	return ictx->scan;
}

static bool
index_scanner_getnext(ScannerCtx *scannerCtx) {
	InternalScannerCtx *internalScannerCtx = &scannerCtx->internal;
	bool success = index_getnext_slot(internalScannerCtx->scan.index_scan, scannerCtx->scandirection, internalScannerCtx->tinfo.slot);
	internalScannerCtx->tinfo.ituple = internalScannerCtx->scan.index_scan->xs_itup;
	internalScannerCtx->tinfo.ituple_desc = internalScannerCtx->scan.index_scan->xs_itupdesc;

	return success;
}

static void
index_scanner_rescan(ScannerCtx *ctx) {
	index_rescan(ctx->internal.scan.index_scan, ctx->scankey, ctx->nkeys, NULL, ctx->norderbys);
}

static void
index_scanner_endscan(ScannerCtx *ctx) {
	index_endscan(ctx->internal.scan.index_scan);
}

static void
index_scanner_close(ScannerCtx *ctx) {
	LOCKMODE lockmode = (ctx->flags & SCANNER_F_KEEPLOCK) ? NoLock : ctx->lockmode;
	index_close(ctx->indexrel, ctx->lockmode);
	table_close(ctx->tablerel, lockmode);
}

/*
 * Two scanners by type: heap and index scanners.
 */
static Scanner scanners[] = {
	[ScannerTypeTable] = {
		.openscan = table_scanner_open,
		.beginscan = table_scanner_beginscan,
		.getnext = table_scanner_getnext,
		.rescan = table_scanner_rescan,
		.endscan = table_scanner_endscan,
		.closescan = table_scanner_close,
	},
	[ScannerTypeIndex] = {
		.openscan = index_scanner_open,
		.beginscan = index_scanner_beginscan,
		.getnext = index_scanner_getnext,
		.rescan = index_scanner_rescan,
		.endscan = index_scanner_endscan,
		.closescan = index_scanner_close,
	}
};

static inline Scanner *scanner_ctx_get_scanner(ScannerCtx *ctx) {
	if (OidIsValid(ctx->index)) {
		return &scanners[ScannerTypeIndex];
	}

	return &scanners[ScannerTypeTable];
}

TSDLLEXPORT void
ts_scanner_rescan(ScannerCtx *ctx, const ScanKey scankey) {
	Scanner *scanner = scanner_ctx_get_scanner(ctx);
	MemoryContext oldmcxt;

	/* If scankey is NULL, the existing scan key was already updated or the
	 * old should be reused */
	if (NULL != scankey)
		memcpy(ctx->scankey, scankey, sizeof(*ctx->scankey));

	oldmcxt = MemoryContextSwitchTo(ctx->internal.scan_mcxt);
	scanner->rescan(ctx);
	MemoryContextSwitchTo(oldmcxt);
}

static void
prepare_scan(ScannerCtx *ctx) {
	ctx->internal.ended = false;
	ctx->internal.registered_snapshot = false;

	if (ctx->internal.scan_mcxt == NULL)
		ctx->internal.scan_mcxt = CurrentMemoryContext;

	if (ctx->snapshot == NULL) {
		/*
		 * We use SnapshotSelf by default, for historical reasons mostly, but
		 * we probably want to move to an MVCC snapshot as the default. The
		 * difference is that a Self snapshot is an "instant" snapshot and can
		 * see its own changes. More importantly, however, unlike an MVCC
		 * snapshot, a Self snapshot is not subject to the strictness of
		 * SERIALIZABLE isolation mode.
		 *
		 * This is important in case of, e.g., concurrent chunk creation by
		 * two transactions; we'd like a transaction to use a new chunk as
		 * soon as the creating transaction commits, so that there aren't
		 * multiple transactions creating the same chunk and all but one fails
		 * with a conflict. However, under SERIALIZABLE mode a transaction is
		 * only allowed to read data from transactions that were committed
		 * prior to transaction start. This means that two or more
		 * transactions that create the same chunk must have all but the first
		 * committed transaction fail.
		 *
		 * Therefore, we probably want to exempt internal bookkeeping metadata
		 * from full SERIALIZABLE semantics (at least in the case of chunk
		 * creation), or otherwise the INSERT behavior will be different for
		 * hypertables compared to regular tables under SERIALIZABLE
		 * mode.
		 */
		MemoryContext oldmcxt = MemoryContextSwitchTo(ctx->internal.scan_mcxt);
		ctx->snapshot = RegisterSnapshot(GetSnapshotData(SnapshotSelf));
		ctx->internal.registered_snapshot = true;
		MemoryContextSwitchTo(oldmcxt);
	}
}

TSDLLEXPORT Relation ts_scanner_open(ScannerCtx *scannerCtx) {
	Scanner *scanner = scanner_ctx_get_scanner(scannerCtx);

	Assert(NULL == scannerCtx->tablerel);

	prepare_scan(scannerCtx);

	Assert(scannerCtx->internal.scan_mcxt != NULL);

	MemoryContext oldmcxt = MemoryContextSwitchTo(scannerCtx->internal.scan_mcxt);
	Relation relation = scanner->openscan(scannerCtx);
	MemoryContextSwitchTo(oldmcxt);

	return relation;
}

/*
 * Start either a heap or index scan depending on the information in the
 * ScannerCtx. ScannerCtx must be setup by caller with the proper information
 * for the scan, including filters and callbacks for found tuples.
 */
TSDLLEXPORT void ts_scanner_start_scan(ScannerCtx *scannerCtx) {
	InternalScannerCtx *internalScannerCtx = &scannerCtx->internal;

	if (internalScannerCtx->started) {
		Assert(!internalScannerCtx->ended);
		Assert(scannerCtx->tablerel);
		Assert(OidIsValid(scannerCtx->table));
		return;
	}

	if (scannerCtx->tablerel == NULL) {
		Assert(NULL == scannerCtx->indexrel);
		ts_scanner_open(scannerCtx);
	} else {
		/*
		 * Relations already opened by caller: Only need to prepare the scan
		 * and set relation Oids so that the scanner knows which scanner
		 * implementation to use. Respect the auto-closing behavior set by the
		 * user, which is to auto close if unspecified.
		 */
		prepare_scan(scannerCtx);
		scannerCtx->table = RelationGetRelid(scannerCtx->tablerel);

		if (NULL != scannerCtx->indexrel)
			scannerCtx->index = RelationGetRelid(scannerCtx->indexrel);
	}

	Assert(scannerCtx->internal.scan_mcxt != NULL);
	MemoryContext oldmcxt = MemoryContextSwitchTo(scannerCtx->internal.scan_mcxt);

	Scanner *scanner = scanner_ctx_get_scanner(scannerCtx);
	scanner->beginscan(scannerCtx);

	TupleDesc tuple_desc = RelationGetDescr(scannerCtx->tablerel);

	internalScannerCtx->tinfo.scanrel = scannerCtx->tablerel;
	internalScannerCtx->tinfo.mctx = scannerCtx->result_mctx == NULL ? CurrentMemoryContext : scannerCtx->result_mctx;
	internalScannerCtx->tinfo.slot = MakeSingleTupleTableSlot(tuple_desc, table_slot_callbacks(scannerCtx->tablerel));
	MemoryContextSwitchTo(oldmcxt);

	/* Call pre-scan handler, if any. */
	if (scannerCtx->prescan != NULL)
		scannerCtx->prescan(scannerCtx->data);

	internalScannerCtx->started = true;
}

static inline bool ts_scanner_limit_reached(ScannerCtx *ctx) {
	return ctx->limit > 0 && ctx->internal.tinfo.count >= ctx->limit;
}

static void
scanner_cleanup(ScannerCtx *ctx) {
	InternalScannerCtx *ictx = &ctx->internal;

	if (ictx->registered_snapshot) {
		UnregisterSnapshot(ctx->snapshot);
		ctx->snapshot = NULL;
	}

	if (NULL != ictx->tinfo.slot) {
		ExecDropSingleTupleTableSlot(ictx->tinfo.slot);
		ictx->tinfo.slot = NULL;
	}

	if (NULL != ictx->scan_mcxt) {
		ictx->scan_mcxt = NULL;
	}
}

TSDLLEXPORT void
ts_scanner_end_scan(ScannerCtx *ctx) {
	InternalScannerCtx *ictx = &ctx->internal;
	Scanner *scanner = scanner_ctx_get_scanner(ctx);
	MemoryContext oldmcxt;

	if (ictx->ended)
		return;

	/* Call post-scan handler, if any. */
	if (ctx->postscan != NULL)
		ctx->postscan(ictx->tinfo.count, ctx->data);

	oldmcxt = MemoryContextSwitchTo(ctx->internal.scan_mcxt);
	scanner->endscan(ctx);
	MemoryContextSwitchTo(oldmcxt);

	scanner_cleanup(ctx);
	ictx->ended = true;
	ictx->started = false;
}

TSDLLEXPORT void
ts_scanner_close(ScannerCtx *ctx) {
	Scanner *scanner = scanner_ctx_get_scanner(ctx);

	Assert(ctx->internal.ended);

	if (NULL != ctx->tablerel) {
		scanner->closescan(ctx);
		ctx->tablerel = NULL;
		ctx->indexrel = NULL;
	}
}

TSDLLEXPORT TupleInfo *ts_scanner_next(ScannerCtx *scannerCtx) {
	InternalScannerCtx *internalScannerCtx = &scannerCtx->internal;
	Scanner *scanner = scanner_ctx_get_scanner(scannerCtx);
	bool isValid = false;

	if (!ts_scanner_limit_reached(scannerCtx)) {
		MemoryContext oldmcxt = MemoryContextSwitchTo(scannerCtx->internal.scan_mcxt);
		isValid = scanner->getnext(scannerCtx);
		MemoryContextSwitchTo(oldmcxt);
	}

	while (isValid) {
		if (scannerCtx->filter == NULL || scannerCtx->filter(&internalScannerCtx->tinfo, scannerCtx->data) == SCAN_INCLUDE) {
			internalScannerCtx->tinfo.count++;

			if (scannerCtx->tuplock) {
				TupleTableSlot *slot = internalScannerCtx->tinfo.slot;

				Assert(scannerCtx->snapshot);
				internalScannerCtx->tinfo.lockresult = table_tuple_lock(scannerCtx->tablerel,
																		&(slot->tts_tid),
																		scannerCtx->snapshot,
																		slot,
																		GetCurrentCommandId(false),
																		scannerCtx->tuplock->lockmode,
																		scannerCtx->tuplock->waitpolicy,
																		scannerCtx->tuplock->lockflags,
																		&internalScannerCtx->tinfo.lockfd);
			}

			/* stop at a valid tuple */
			return &internalScannerCtx->tinfo;
		}

		if (ts_scanner_limit_reached(scannerCtx))
			isValid = false;
		else {
			MemoryContext oldmcxt = MemoryContextSwitchTo(scannerCtx->internal.scan_mcxt);
			isValid = scanner->getnext(scannerCtx);
			MemoryContextSwitchTo(oldmcxt);
		}
	}

	if (!(scannerCtx->flags & SCANNER_F_NOEND))
		ts_scanner_end_scan(scannerCtx);

	if (!(scannerCtx->flags & SCANNER_F_NOEND_AND_NOCLOSE))
		ts_scanner_close(scannerCtx);

	return NULL;
}

/**
 * Perform either a heap or index scan depending on the information in the ScannerCtx.<br>
 * ScannerCtx must be setup by caller with the proper information for the scan, including filters and callbacks for found tuples.
 * @return the number of tuples that were found
 */
TSDLLEXPORT int ts_scanner_scan(ScannerCtx *scannerCtx) {
	MemSet(&scannerCtx->internal, 0, sizeof(scannerCtx->internal));

	TupleInfo *tupleInfo;

	for (ts_scanner_start_scan(scannerCtx);
		 (tupleInfo = ts_scanner_next(scannerCtx));) {
		/* Call tuple_found handler. Abort the scan if the handler wants us to */
		if (scannerCtx->tuple_found != NULL &&
			scannerCtx->tuple_found(tupleInfo, scannerCtx->data) == SCAN_DONE) {
			if (!(scannerCtx->flags & SCANNER_F_NOEND))
				ts_scanner_end_scan(scannerCtx);

			if (!(scannerCtx->flags & SCANNER_F_NOEND_AND_NOCLOSE))
				ts_scanner_close(scannerCtx);

			break;
		}
	}

	return scannerCtx->internal.tinfo.count;
}

TSDLLEXPORT bool ts_scanner_scan_one(ScannerCtx *ctx, bool fail_if_not_found, const char *item_type) {
	int num_found = ts_scanner_scan(ctx);

	ctx->limit = 2;

	switch (num_found) {
		case 0:
			if (fail_if_not_found) {
				elog(ERROR, "%s not found", item_type);
			}
			return false;
		case 1:
			return true;
		default:
			elog(ERROR, "more than one %s found", item_type);
			return false;
	}
}

ItemPointer
ts_scanner_get_tuple_tid(TupleInfo *ti) {
	return &ti->slot->tts_tid;
}

HeapTuple
ts_scanner_fetch_heap_tuple(const TupleInfo *ti, bool materialize, bool *should_free) {
	return ExecFetchSlotHeapTuple(ti->slot, materialize, should_free);
}

TupleDesc
ts_scanner_get_tupledesc(const TupleInfo *ti) {
	return ti->slot->tts_tupleDescriptor;
}

void *
ts_scanner_alloc_result(const TupleInfo *ti, Size size) {
	return MemoryContextAllocZero(ti->mctx, size);
}
