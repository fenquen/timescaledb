/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/namespace.h>
#include <utils/catcache.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>

#include "errors.h"
#include "hypertable_cache.h"
#include "hypertable.h"
#include "ts_catalog/catalog.h"
#include "cache.h"
#include "scanner.h"
#include "dimension.h"
#include "ts_catalog/tablespace.h"

static void *hypertable_cache_create_entry(Cache *cache, CacheQuery *cacheQuery);
static void hypertable_cache_missing_error(const Cache *cache, const CacheQuery *query);

// cache的获取请求
typedef struct HypertableCacheQuery {
	CacheQuery cacheQuery;
	Oid relid; // targetTableOid
	const char *schema;
	const char *table;
} HypertableCacheQuery;

static void *hypertable_cache_get_key(CacheQuery *cacheQuery) {
	return &((HypertableCacheQuery *) cacheQuery)->relid;
}

// 代表1个缓存条目
typedef struct {
	Oid relid;
	Hypertable *hypertable;
} HypertableCacheEntry;

static bool
hypertable_cache_valid_result(const void *result) {
	if (result == NULL)
		return false;
	return ((HypertableCacheEntry *) result)->hypertable != NULL;
}

static Cache *
hypertable_cache_create() {
	MemoryContext ctx =
		AllocSetContextCreate(CacheMemoryContext, "Hypertable cache", ALLOCSET_DEFAULT_SIZES);

	Cache *cache = MemoryContextAlloc(ctx, sizeof(Cache));
	Cache template = {
		.hashctl = {
			.keysize = sizeof(Oid),
			.entrysize = sizeof(HypertableCacheEntry),
			.hcxt = ctx,
		},
		.name = "hypertable_cache",
		.numelements = 16,
		.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
		.get_key = hypertable_cache_get_key,
		.create_entry = hypertable_cache_create_entry,
		.missing_error = hypertable_cache_missing_error,
		.valid_result = hypertable_cache_valid_result,
	};

	*cache = template;

	ts_cache_init(cache);

	return cache;
}

static Cache *hypertable_cache_current = NULL;

static ScanTupleResult hypertable_tuple_found(TupleInfo *tupleInfo, void *data) {
	HypertableCacheEntry *entry = data;

	entry->hypertable = ts_hypertable_from_tupleinfo(tupleInfo);
	return SCAN_DONE;
}

static void *hypertable_cache_create_entry(Cache *cache, CacheQuery *cacheQuery) {
	HypertableCacheQuery *hypertableCacheQuery = (HypertableCacheQuery *) cacheQuery;
	HypertableCacheEntry *hypertableCacheEntry = cacheQuery->result;

	if (NULL == hypertableCacheQuery->schema) {
		hypertableCacheQuery->schema = get_namespace_name(get_rel_namespace(hypertableCacheQuery->relid));
	}

	if (NULL == hypertableCacheQuery->table) {
		hypertableCacheQuery->table = get_rel_name(hypertableCacheQuery->relid);
	}

	// 应该是去scan  _timescaledb_catalog.hypertable表
	int number_found = ts_hypertable_scan_with_memory_context(hypertableCacheQuery->schema,
															  hypertableCacheQuery->table,
															  hypertable_tuple_found,
															  cacheQuery->result,
															  AccessShareLock,
															  false,
															  ts_cache_memory_ctx(cache));

	switch (number_found) {
		case 0:
			/* Negative cache entry: table is not a hypertable */
			hypertableCacheEntry->hypertable = NULL;
			break;
		case 1:
			Assert(strncmp(hypertableCacheEntry->hypertable->fd.schema_name.data, hypertableCacheQuery->schema, NAMEDATALEN) == 0);
			Assert(strncmp(hypertableCacheEntry->hypertable->fd.table_name.data, hypertableCacheQuery->table, NAMEDATALEN) == 0);
			break;
		default:
			elog(ERROR, "got an unexpected number of records: %d", number_found);
			break;
	}

	return hypertableCacheEntry->hypertable == NULL ? NULL : hypertableCacheEntry;
}

static void hypertable_cache_missing_error(const Cache *cache, const CacheQuery *query) {
	HypertableCacheQuery *hq = (HypertableCacheQuery *) query;
	const char *const rel_name = get_rel_name(hq->relid);

	if (rel_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("OID %u does not refer to a table", hq->relid)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable", rel_name)));
}

void ts_hypertable_cache_invalidate_callback(void) {
	ts_cache_invalidate(hypertable_cache_current);
	hypertable_cache_current = hypertable_cache_create();
}

/* Get hypertable cache entry. If the entry is not in the cache, add it. */
Hypertable *ts_hypertable_cache_get_entry(Cache *const cache,
										  const Oid targetTableOid,
										  const unsigned int cacheQueryFlags) {
	if (!OidIsValid(targetTableOid)) {
		if (cacheQueryFlags & CACHE_FLAG_MISSING_OK) {
			return NULL;
		}

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid Oid")));
	}

	return ts_hypertable_cache_get_entry_with_table(cache, targetTableOid, NULL, NULL, cacheQueryFlags);
}

Hypertable *ts_hypertable_cache_get_entry_with_table(Cache *cache,
													 const Oid targetTableOid,
													 const char *schemaName,
													 const char *tableName,
													 const unsigned int cacheQueryFlags) {
	// 组装cache的获取请求
	HypertableCacheQuery hypertableCacheQuery = {
		.cacheQuery.flags = cacheQueryFlags,
		.relid = targetTableOid,
		.schema = schemaName,
		.table = tableName,
	};

	// 使用该请求获取cache
	HypertableCacheEntry *hypertableCacheEntry = ts_cache_fetch(cache, &hypertableCacheQuery.cacheQuery);
	Assert((cacheQueryFlags & CACHE_FLAG_MISSING_OK) ? true : (hypertableCacheEntry != NULL && hypertableCacheEntry->hypertable != NULL));
	return hypertableCacheEntry == NULL ? NULL : hypertableCacheEntry->hypertable;
}

/*
 * Returns cache into the argument and hypertable as the function result.
 * If hypertable is not found, fails with an error.
 */
Hypertable *ts_hypertable_cache_get_cache_and_entry(const Oid targetTableOid,
													const unsigned int cacheQueryFlags,
													Cache **const cache) {
	// 得到当全局的 hypertable_cache_current 把它放到 cache_pin 再把 cache_pin 加到 pinned_caches


	*cache = ts_hypertable_cache_pin();

	return ts_hypertable_cache_get_entry(*cache, targetTableOid, cacheQueryFlags);
}

Hypertable *
ts_hypertable_cache_get_entry_rv(Cache *cache, const RangeVar *rv) {
	return ts_hypertable_cache_get_entry(cache, RangeVarGetRelid(rv, NoLock, true), true);
}

TSDLLEXPORT Hypertable *
ts_hypertable_cache_get_entry_by_id(Cache *cache, const int32 hypertable_id) {
	return ts_hypertable_cache_get_entry(cache, ts_hypertable_id_to_relid(hypertable_id), true);
}

extern TSDLLEXPORT Cache *ts_hypertable_cache_pin() {
	return ts_cache_pin(hypertable_cache_current);
}

void _hypertable_cache_init(void) {
	CreateCacheMemoryContext();
	hypertable_cache_current = hypertable_cache_create();
}

void _hypertable_cache_fini(void) {
	ts_cache_invalidate(hypertable_cache_current);
}
