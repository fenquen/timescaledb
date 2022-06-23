/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/memutils.h>

#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypercube.h"
#include "subspace_store.h"


/*
 * In terms of datastructures, the subspace store is actually a tree. At the
 * root of a tree is a DimensionVec representing the different DimensionSlices
 * for the first dimension. Each of the DimensionSlices of the
 * first dimension point to a DimensionVec of the second dimension. This recurses
 * for the N dimensions. The leaf DimensionSlice points to the data being stored.
 *
 */
typedef struct SubspaceStoreInternalNode {
	DimensionVec *vector; // 不是含有多个dimension 而是汇集了单个dimension麾下的多个slice
	size_t descendants;
	bool last_internal_node;
} SubspaceStoreInternalNode;

typedef struct SubspaceStore {
	MemoryContext mcxt;
	int16 num_dimensions;
	/* limit growth of store by  limiting number of slices in first dimension,	0 for no limit */
	int16 max_items;
	SubspaceStoreInternalNode *origin; /* origin of the tree */
} SubspaceStore;

static inline SubspaceStoreInternalNode *
subspace_store_internal_node_create(bool last_internal_node) {
	SubspaceStoreInternalNode *node = palloc(sizeof(SubspaceStoreInternalNode));

	node->vector = ts_dimension_vec_create(DIMENSION_VEC_DEFAULT_SIZE);
	node->descendants = 0;
	node->last_internal_node = last_internal_node;
	return node;
}

static inline void
subspace_store_internal_node_free(void *node) {
	ts_dimension_vec_free(((SubspaceStoreInternalNode *) node)->vector);
	pfree(node);
}

static size_t
subspace_store_internal_node_descendants(SubspaceStoreInternalNode *node, int index) {
	const DimensionSlice *slice = ts_dimension_vec_get(node->vector, index);

	if (slice == NULL)
		return 0;

	if (node->last_internal_node)
		return 1;

	return ((SubspaceStoreInternalNode *) slice->storage)->descendants;
}

SubspaceStore *
ts_subspace_store_init(const Hyperspace *space, MemoryContext mcxt, int16 max_items) {
	MemoryContext old = MemoryContextSwitchTo(mcxt);
	SubspaceStore *sst = palloc(sizeof(SubspaceStore));

	/*
	 * make sure that the first dimension is a time dimension, otherwise the
	 * tree will grow in a way that makes pruning less effective.
	 */
	Assert(space->num_dimensions < 1 || space->dimensions[0].type == DIMENSION_TYPE_OPEN);

	sst->origin = subspace_store_internal_node_create(space->num_dimensions == 1);
	sst->num_dimensions = space->num_dimensions;
	/* max_items = 0 is treated as unlimited */
	sst->max_items = max_items;
	sst->mcxt = mcxt;
	MemoryContextSwitchTo(old);
	return sst;
}

// 得到对应dimension slice
void ts_subspace_store_add(SubspaceStore *subspaceStore, // 含有dimension数量
						   const Hypercube *hypercube,	 // 含有slice数量
						   void *object,				 // chunkInsertState
						   void (*object_free)(void *)) {
	SubspaceStoreInternalNode *subspaceStoreInternalNode = subspaceStore->origin;
	DimensionSlice *last = NULL;

	MemoryContext old = MemoryContextSwitchTo(subspaceStore->mcxt);

	Assert(hypercube->num_slices == subspaceStore->num_dimensions);

	// 遍历各个slice
	for (int i = 0; i < hypercube->num_slices; i++) {
		const DimensionSlice *dimensionSlice = hypercube->slices[i];

		Assert(dimensionSlice->storage == NULL);

		if (subspaceStoreInternalNode == NULL) {
			/*
			 * We should have one internal subspaceStoreInternalNode per dimension in the
			 * hypertable. If we don't have one for the current dimension,
			 * create one now. (There will always be one for time)
			 */
			Assert(last != NULL);
			last->storage = subspace_store_internal_node_create(i == (hypercube->num_slices - 1));
			last->storage_free = subspace_store_internal_node_free;
			subspaceStoreInternalNode = last->storage;
		}

		Assert(subspaceStore->max_items == 0 || subspaceStoreInternalNode->descendants <= (size_t) subspaceStore->max_items);

		/*
		 * We only call this function on a cache miss, so number of leaves
		 * will definitely increase see `Assert(last != NULL && last->storage == NULL);` at bottom.
		 */
		subspaceStoreInternalNode->descendants += 1;

		Assert(0 == subspaceStoreInternalNode->vector->num_slices ||
			   subspaceStoreInternalNode->vector->slices[0]->fd.dimension_id == dimensionSlice->fd.dimension_id);

		// do we have enough space to subspaceStore the object? 增加1之后过最大的边线
		// 意思应该是 这个hypertable在设置的时候配置了最多保留多少个slice  对应多少数量的chunk
		if (subspaceStore->max_items > 0 && subspaceStoreInternalNode->descendants > subspaceStore->max_items) {
			/*
			 * Always delete the slice corresponding to the earliest time  把老的干掉
			 * range. In the normal case that inserts are performed in
			 * time-order this is the one least likely to be reused. (Note
			 * that we made sure that the first dimension is a time dimension
			 * when creating the subspace_store). If out-of-order inserts are
			 * become significant we may wish to change this to something more sophisticated like LRU.
			 */
			size_t items_removed = subspace_store_internal_node_descendants(subspaceStoreInternalNode, i);

			/*
			 * descendants at the root is inclusive of the descendants at the
			 * children, so if we have an overflow it must be in the time dim
			 */
			Assert(i == 0);

			Assert(subspaceStore->max_items + 1 == subspaceStoreInternalNode->descendants);

			ts_dimension_vec_remove_slice(&subspaceStoreInternalNode->vector, i);

			// Note we would have to do this to ancestors if this was not the root.
			subspaceStoreInternalNode->descendants -= items_removed;
		}

		DimensionSlice *match = ts_dimension_vec_find_slice(subspaceStoreInternalNode->vector, dimensionSlice->fd.range_start);

		/* do we have a slot in this vector for the new object? */
		if (match == NULL) {
			// create a new copy of the range this slice covers, to subspaceStore the object in
			DimensionSlice *copy = ts_dimension_slice_copy(dimensionSlice);

			ts_dimension_vec_add_slice_sort(&subspaceStoreInternalNode->vector, copy);
			match = copy;
		}

		Assert(subspaceStore->max_items == 0 || subspaceStoreInternalNode->descendants <= (size_t) subspaceStore->max_items);

		last = match; // subspaceStoreInternalNode->(match->last->storage->subspaceStoreInternalNode)
		/* internal slices point to the next SubspaceStoreInternalNode */
		subspaceStoreInternalNode = last->storage;
	}

	Assert(last != NULL && last->storage == NULL);

	last->storage = object; /* at the end we subspaceStore the object */
	last->storage_free = object_free;

	MemoryContextSwitchTo(old);
}

void *ts_subspace_store_get(const SubspaceStore *subspaceStore, const Point *target) {
	DimensionVec *dimensionVec = subspaceStore->origin->vector;
	DimensionSlice *match = NULL;

	Assert(target->cardinality == subspaceStore->num_dimensions);

	/* The internal compressed hypertable has no dimensions as
	 * chunks are created explicitly by compress_chunk and linked to the source chunk. */
	if (subspaceStore->num_dimensions == 0) {
		return NULL;
	}

	for (int i = 0; i < target->cardinality; i++) {
		match = ts_dimension_vec_find_slice(dimensionVec, target->coordinates[i]);

		if (NULL == match) {
			return NULL;
		}

		dimensionVec = ((SubspaceStoreInternalNode *) match->storage)->vector;
	}

	Assert(match != NULL);

	return match->storage;
}

void ts_subspace_store_free(SubspaceStore *store) {
	subspace_store_internal_node_free(store->origin);
	pfree(store);
}

MemoryContext
ts_subspace_store_mcxt(const SubspaceStore *store) {
	return store->mcxt;
}
