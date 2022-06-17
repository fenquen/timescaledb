/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CACHE_H
#define TIMESCALEDB_CACHE_H

#include <postgres.h>
#include <utils/memutils.h>
#include <utils/hsearch.h>

#include "export.h"

typedef enum CacheQueryFlags {
	CACHE_FLAG_NONE = 0,
	CACHE_FLAG_MISSING_OK = 1 << 0,
	CACHE_FLAG_NOCREATE = 1 << 1,
} CacheQueryFlags;

#define CACHE_FLAG_CHECK (CACHE_FLAG_MISSING_OK | CACHE_FLAG_NOCREATE)


// 代表了获取缓存的请求 得到的果实保存在result
typedef struct CacheQuery {
	// 对应CacheQueryFlags
	const unsigned int flags;

	void *result;

	// java nio中的selection key的attachment
	void *data;
} CacheQuery;

typedef struct CacheStats {
	long numelements;
	uint64 hits;
	uint64 misses;
} CacheStats;

typedef struct Cache {
	HASHCTL hashctl;
	HTAB *htab;
	int refcount;
	const char *name;
	long numelements;
	int flags;
	CacheStats stats;
	/**
	 * 目前已知对应 hypertable_cache_get_key -> &((HypertableCacheQuery *) cacheQuery)->relid <br>
	 * 其实是目标表的oid
	 */
	void *(*get_key)(struct CacheQuery *);

	// hypertable_cache_create_entry
	void *(*create_entry)(struct Cache *, CacheQuery *);
	void *(*update_entry)(struct Cache *, CacheQuery *);
	// 对应cache missing时候的处理
	void (*missing_error)(const struct Cache *, const CacheQuery *);
	bool (*valid_result)(const void *);
	void (*remove_entry)(void *entry);
	void (*pre_destroy_hook)(struct Cache *);

	// Auto-release caches on (sub)txn aborts and commits. Should be off if cache used in txn callbacks
	bool handle_txn_callbacks;

	// This should be false if doing cross-commit operations like CLUSTER or VACUUM
	bool release_on_commit;
} Cache;

extern TSDLLEXPORT void ts_cache_init(Cache *cache);
extern TSDLLEXPORT void ts_cache_invalidate(Cache *cache);
extern TSDLLEXPORT void *ts_cache_fetch(Cache *cache, CacheQuery *cacheQuery);
extern TSDLLEXPORT bool ts_cache_remove(Cache *cache, void *key);

extern MemoryContext ts_cache_memory_ctx(Cache *cache);

extern TSDLLEXPORT Cache *ts_cache_pin(Cache *cache);
extern TSDLLEXPORT int ts_cache_release(Cache *cache);

extern void _cache_init(void);
extern void _cache_fini(void);

#endif /* TIMESCALEDB_CACHE_H */
