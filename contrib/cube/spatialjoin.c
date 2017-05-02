#include "spatialjoin.h"



#include "access/gist.h"
#include "access/gist_private.h"
#include "utils/memutils.h"
#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif
#include "catalog/namespace.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_cast.h"

#if PG_VERSION_NUM >= 90600
#define heap_formtuple heap_form_tuple
#endif



static void initStack(Stack*queue)
{
	queue->head_index = 0;
	queue->head = palloc(sizeof(StackSegment));
	queue->backlog = NULL;
}

static void
stackPush(Stack* queue, PendingPair pair)
{
	queue->head->Data[queue->head_index] = pair;
	queue->head_index++;
	queue->count++;

	if(queue->head_index == QueueSegmentSize)
	{
		StackSegment* newHead;
		if(queue->backlog)
		{
			newHead = queue->backlog;
			queue->backlog = queue->backlog->NextSegment;
		}
		else
		{
			newHead = palloc(sizeof(StackSegment));
		}
		newHead->NextSegment = queue->head;
		queue->head = newHead;

		queue->head_index = 0;
	}
}

static PendingPair
stackPop(Stack* queue)
{
	PendingPair pair;
	if(queue->head_index == 0)
		{
		StackSegment* newHead = queue->head->NextSegment;
			queue->head->NextSegment = queue->backlog;
			queue->backlog = queue->head;
			queue->head = newHead;
			queue->head_index = QueueSegmentSize;
		}

	queue->head_index--;
	pair = queue->head->Data[queue->head_index];

	queue->count--;

	return pair;
}

static void initQueue(Queue*queue)
{
	queue->head_index = 0;
	queue->tail_index = 0;
	queue->head = queue->tail = palloc(sizeof(QueueSegment));
	queue->backlog = NULL;
}

static void
enqueue(Queue* queue, ResultPair pair)
{
	queue->tail->Data[queue->tail_index] = pair;
	queue->tail_index++;
	queue->count++;
	if(queue->tail_index == QueueSegmentSize)
	{
		if(queue->backlog)
		{
			queue->tail->NextSegment = queue->backlog;
			queue->backlog = queue->backlog->NextSegment;
		}
		else
		{
			queue->tail->NextSegment = palloc(sizeof(QueueSegment));
		}

		queue->tail = queue->tail->NextSegment;
		queue->tail_index = 0;
	}
}

static ResultPair
dequeue(Queue* queue)
{
	ResultPair pair = queue->head->Data[queue->head_index];

	queue->head_index++;
	queue->count--;
	if(queue->head_index == QueueSegmentSize)
	{
		QueueSegment* newHead = queue->head->NextSegment;
		queue->head->NextSegment = queue->backlog;
		queue->backlog = queue->head;
		queue->head = newHead;
		queue->head_index = 0;

	}

	return pair;
}


/*
 * Add pending pages pair to context.
 */
static void
addPendingPair(CrossmatchContext *ctx, BlockNumber blk1, BlockNumber blk2,
			   GistNSN parentlsn1, GistNSN parentlsn2,NDBOX*box1,NDBOX*box2)
{
	PendingPair blockNumberPair;

	/* Add pending pair */
	blockNumberPair.blk1 = blk1;
	blockNumberPair.blk2 = blk2;
	blockNumberPair.parentlsn1 = parentlsn1;
	blockNumberPair.parentlsn2 = parentlsn2;
	blockNumberPair.box1 = box1;
	blockNumberPair.box2 = box2;
	stackPush(&ctx->pendingPairs,blockNumberPair);
}

/*
 * Add result item pointers pair to context.
 */
static void
addResultPair(CrossmatchContext *ctx, ItemPointer iptr1, ItemPointer iptr2)
{
	ResultPair itemPointerPair;

	/* Add result pair */

	itemPointerPair.iptr1 = *iptr1;
	itemPointerPair.iptr2 = *iptr2;
	enqueue(&ctx->resultsPairs, itemPointerPair);
}


/*
 * Close index relation opened with AccessShareLock.
 */
static void
indexClose(Relation r)
{
	index_close((r), AccessShareLock);
}


void
setupFirstcallNode(CrossmatchContext *ctx, Oid idx1, Oid idx2)
{
	GistNSN		parentnsn = InvalidNSN;

	ctx->box = NULL;

	Assert(idx1 != idx2);

	ctx->indexes[0] = index_open(idx1, AccessShareLock);
	ctx->indexes[1] = index_open(idx2, AccessShareLock);
	initStack(&ctx->pendingPairs);
	initQueue(&ctx->resultsPairs);

	/*
	 * Add first pending pair of pages: we start scan both indexes from their
	 * roots.
	 */
	addPendingPair(ctx, GIST_ROOT_BLKNO, GIST_ROOT_BLKNO,
				   parentnsn, parentnsn,NULL,NULL);
}


void endCall(CrossmatchContext *ctx)
{
	indexClose(ctx->indexes[0]);
	indexClose(ctx->indexes[1]);
}



static NDBOX *
cube_union_b3(Box3DInfo *a, int n)
{
	int			i, o;
	NDBOX	   *result;
	int			size;
	int			dim = 0;


	for (i = 0; i < n; i++)
		{
			if (DIM(a[i].cube) > dim)
				dim = DIM(a[i].cube);
		}

	size = CUBE_SIZE(dim);
	result = palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, dim);

	for (i = 0; i < dim; i++)
	{
		result->x[i] = DBL_MAX;
		result->x[i + dim] = DBL_MIN;
	}

	for (o = 0; o < n; o++)
		for (i = 0; i < dim; i++)
		{
			if (DIM(a[o].cube) <= i)
				break;
			result->x[i] = Min(
				Min(LL_COORD(a[o].cube, i), UR_COORD(a[o].cube, i)),
				LL_COORD(result, i)
				);
			result->x[i + dim] = Max(
				Max(LL_COORD(a[o].cube, i), UR_COORD(a[o].cube, i)),
				UR_COORD(result, i)
				);
		}
	/*
	for (i = 0; i < dim; i++)
	{
		if(result->x[i] == DBL_MAX && result->x[i + dim] == DBL_MIN)
		{
			result->x[i] = 0;
			result->x[i + dim] = 0;
		}
	}
*/
	/*
	* Check if the result was in fact a point, and set the flag in the datum
	* accordingly. (we don't bother to repalloc it smaller)
	*/
	/*
	if (cube_is_point_internal(result))
	{
		size = POINT_SIZE(dim);
		SET_VARSIZE(result, size);
		SET_POINT_BIT(result);
	}*/

	return (result);
}

static NDBOX *
cube_union_pi(PointInfo *a, int n)
{
	int			i, o;
	NDBOX	   *result;
	int			size;
	int			dim = 0;


	for (i = 0; i < n; i++)
		{
			if (DIM(a[i].cube) > dim)
				dim = DIM(a[i].cube);
		}

	size = CUBE_SIZE(dim);
	result = palloc0(size);
	SET_VARSIZE(result, size);
	SET_DIM(result, dim);

	for (i = 0; i < dim; i++)
	{
		result->x[i] = DBL_MAX;
		result->x[i + dim] = DBL_MIN;
	}

	for (o = 0; o < n; o++)
		for (i = 0; i < dim; i++)
		{
			result->x[i] = Min(
				LL_COORD(a[o].cube, i),
				LL_COORD(result, i)
				);
			result->x[i + dim] = Max(
				UR_COORD(a[o].cube, i),
				UR_COORD(result, i)
				);
		}
	/*
	for (i = 0; i < dim; i++)
	{
		if(result->x[i] == DBL_MAX && result->x[i + dim] == DBL_MIN)
		{
			result->x[i] = 0;
			result->x[i + dim] = 0;
		}
	}*/

	/*
	* Check if the result was in fact a point, and set the flag in the datum
	* accordingly. (we don't bother to repalloc it smaller)
	*/
	/*
	if (cube_is_point_internal(result))
	{
		size = POINT_SIZE(dim);
		SET_VARSIZE(result, size);
		SET_POINT_BIT(result);
	}
	*/

	return (result);
}

static NDBOX*
copyCube(NDBOX*cube)
{
	size_t size = IS_POINT(cube)? POINT_SIZE(DIM(cube)):CUBE_SIZE(DIM(cube));
	NDBOX* result = palloc(size);
	memmove(result,cube,size);
	return result;
}

/*
 * Line sweep algorithm on points for find resulting pairs.
 */
static void
pointLineSweep(CrossmatchContext *ctx, PointInfo *points1, int count1,
			   PointInfo *points2, int count2, MemoryContext workContext, NDBOX* box1, NDBOX*box2)
{
	int	i1,i2;
	int i3 = 0;
	NDBOX* x1 = box1;
	NDBOX* x2 = box2;
	if(!x1)
		x1 = cube_union_pi(points1,count1);
	if(!x2)
		x2 = cube_union_pi(points2,count2);
	MemoryContextSwitchTo(workContext);
	for (i2 = 0; i2 < count2; i2++)
	{
		if(cube_overlap_v0(x1,points2[i2].cube))
		{
			points2[i3]	= points2[i2];
			i3++;
		}
	}


	for (i1 = 0; i1 < count1; i1++)
	{
		if(!cube_overlap_v0(points1[i1].cube,x2))
			continue;

		for (i2 = 0; i2 < i3; i2++)
				{
					if(cube_overlap_v0(points1[i1].cube,points2[i2].cube))
					{
						addResultPair(ctx, &points1[i1].iptr, &points2[i2].iptr);
					}
				}
	}

	pfree(x1);
	pfree(x2);
}

/*
 * Fill information about point for line sweep algorithm.
 */
static bool
fillPointInfo(PointInfo *point, CrossmatchContext *ctx, IndexTuple itup,
			  int num)
{
	NDBOX *key;
	Datum		val;
	bool		isnull;

	/* Get index key value */
	val = index_getattr(itup, 1, ctx->indexes[num - 1]->rd_att, &isnull);

	/* Skip if null */
	if (isnull)
		return false;

	key = DatumGetNDBOX(val);


	point->cube = key;
	point->iptr = itup->t_tid;

	return true;
}

/*
 * Line sweep algorithm on 3D boxes for find pending pairs.
 */
static void
box3DLineSweep(CrossmatchContext *ctx, Box3DInfo *boxes1, int count1,
			   Box3DInfo *boxes2, int count2, MemoryContext workContext, NDBOX* box1, NDBOX*box2)
{
	int	i1,i2;


	int i3 = 0;
	NDBOX* x1 = box1;
	NDBOX* x2 = box2;
	if(!x1)
		x1 = cube_union_b3(boxes1,count1);
	if(!x2)
		x2 = cube_union_b3(boxes2,count2);
	MemoryContextSwitchTo(workContext);

	for (i2 = 0; i2 < count2; i2++)
	{
		if(cube_overlap_v0(x1,boxes2[i2].cube))
		{
			boxes2[i3]	= boxes2[i2];
			i3++;
		}
	}


	for (i1 = 0; i1 < count1; i1++)
	{

		if(!cube_overlap_v0(boxes1[i1].cube,x2))
			continue;

		for (i2 = 0; i2 < i3; i2++)
				{
					if(cube_overlap_v0(boxes1[i1].cube,boxes2[i2].cube))
					{
						addPendingPair(ctx, boxes1[i1].blk, boxes2[i2].blk,
									   boxes1[i1].parentlsn, boxes2[i2].parentlsn, copyCube(boxes1[i1].cube), copyCube(boxes2[i2].cube));
					}
				}
	}

	pfree(x1);
	pfree(x2);
}

/*
 * Fill information about 3D box for line sweep algorithm.
 */
static bool
fillBox3DInfo(Box3DInfo *box3d, CrossmatchContext *ctx, IndexTuple itup,
			  int num, GistNSN parentlsn)
{
	NDBOX *key;
	Datum		val;
	bool		isnull;

	/* Get index key value */
	val = index_getattr(itup, 1, ctx->indexes[num - 1]->rd_att, &isnull);

	/* Skip if null */
	if (isnull)
		return false;

	key = DatumGetNDBOX(val);

	box3d->cube = key;
	box3d->blk = ItemPointerGetBlockNumber(&itup->t_tid);
	box3d->parentlsn = parentlsn;

	return true;
}

/*
 * Scan internal page adding corresponding pending pages with its children and
 * given otherblk.
 */
static void
scanForPendingPages(CrossmatchContext *ctx, Buffer *buf, BlockNumber otherblk,
	   int num, GistNSN parentlsn, GistNSN otherParentlsn,NDBOX*box1,NDBOX*box2)
{
	Page		page;
	OffsetNumber maxoff,
				i;
	GISTPageOpaque opaque;

	for (;;)
	{
		/* Get page */
		page = BufferGetPage(*buf);
		opaque = GistPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		/* Iterate over index tuples producing pending pages */
		for (i = FirstOffsetNumber; i <= maxoff; i++)
		{
			ItemId		iid = PageGetItemId(page, i);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);
			BlockNumber childblkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
			bool		isnull;

			/* Get index key value */
			index_getattr(idxtuple, 1, ctx->indexes[num - 1]->rd_att, &isnull);

			/* Skip if null */
			if (isnull)
				continue;

			/* All checks passed: add pending page pair */
			if (num == 1)
			{
				addPendingPair(ctx, childblkno, otherblk,
							   PageGetLSN(page), otherParentlsn, box1, box2);
			}
			else
			{
				addPendingPair(ctx, otherblk, childblkno,
							   otherParentlsn, PageGetLSN(page), box1, box2);
			}
		}

		/* Traverse to next page if needed according to given parentlsn */
		if (GIST_SCAN_FOLLOW_RIGHT(parentlsn, page))
		{
			BlockNumber rightlink = opaque->rightlink;

			UnlockReleaseBuffer(*buf);
			*buf = ReadBuffer(ctx->indexes[num - 1], rightlink);
			LockBuffer(*buf, GIST_SHARE);
		}
		else
		{
			break;
		}
	}
}

/*
 * Read index tuples of given leaf page into PointInfo structures.
 */
static PointInfo *
readPoints(CrossmatchContext *ctx, Buffer *buf, GistNSN parentlsn, int num,
		   int *count)
{
	Page		page;
	OffsetNumber maxoff,
				i;
	GISTPageOpaque opaque;
	PointInfo  *points = NULL;
	int			j = 0,
				allocated = 0;

	for (;;)
	{
		/* Get page */
		page = BufferGetPage(*buf);
		opaque = GistPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		/* Allocate memory for result */
		if (!points)
		{
			points = (PointInfo *) palloc(sizeof(PointInfo) * maxoff);
			allocated = maxoff;
		}
		else if (j + maxoff > allocated)
		{
			allocated = j + maxoff;
			points = (PointInfo *) repalloc(points, sizeof(PointInfo) * allocated);
		}

		/* Iterate over page filling PointInfo structures */
		for (i = FirstOffsetNumber; i <= maxoff; i++)
		{
			ItemId		iid = PageGetItemId(page, i);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

			if (fillPointInfo(&points[j], ctx, idxtuple, num))
				j++;
		}

		/* Traverse to next page if needed according to given parentlsn */
		if (GIST_SCAN_FOLLOW_RIGHT(parentlsn, page))
		{
			BlockNumber rightlink = opaque->rightlink;

			UnlockReleaseBuffer(*buf);
			*buf = ReadBuffer(ctx->indexes[num - 1], rightlink);
			LockBuffer(*buf, GIST_SHARE);
		}
		else
		{
			break;
		}
	}
	*count = j;
	return points;
}

/*
 * Read index tuples of given internal page into Box3DInfo structures.
 */
static Box3DInfo *
readBoxes(CrossmatchContext *ctx, Buffer *buf, GistNSN parentlsn, int num,
		  int *count)
{
	Page		page;
	OffsetNumber maxoff,
				i;
	GISTPageOpaque opaque;
	Box3DInfo  *boxes = NULL;
	int			j = 0,
				allocated = 0;

	for (;;)
	{
		/* Get page */
		page = BufferGetPage(*buf);
		opaque = GistPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		/* Allocate memory for result */
		if (!boxes)
		{
			boxes = (Box3DInfo *) palloc(sizeof(Box3DInfo) * maxoff);
			allocated = maxoff;
		}
		else if (j + maxoff > allocated)
		{
			allocated = j + maxoff;
			boxes = (Box3DInfo *) repalloc(boxes, sizeof(Box3DInfo) * allocated);
		}

		/* Iterate over page filling Box3DInfo structures */
		for (i = FirstOffsetNumber; i <= maxoff; i++)
		{
			ItemId		iid = PageGetItemId(page, i);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

			if (fillBox3DInfo(&boxes[j], ctx, idxtuple, num, PageGetLSN(page)))
				j++;
		}

		/* Traverse to next page if needed according to given parentlsn */
		if (GIST_SCAN_FOLLOW_RIGHT(parentlsn, page))
		{
			BlockNumber rightlink = opaque->rightlink;

			UnlockReleaseBuffer(*buf);
			*buf = ReadBuffer(ctx->indexes[num - 1], rightlink);
			LockBuffer(*buf, GIST_SHARE);
		}
		else
		{
			break;
		}
	}
	*count = j;
	return boxes;
}



/*
 * Process pending page pair producing result pairs or more pending pairs.
 */
static void
processPendingPair(CrossmatchContext *ctx, BlockNumber blk1, BlockNumber blk2,
				   GistNSN parentlsn1, GistNSN parentlsn2,NDBOX*box1,NDBOX*box2)
{
	Buffer		buf1,
				buf2;
	Page		page1,
				page2;

	/* Read and lock both pages */
	buf1 = ReadBuffer(ctx->indexes[0], blk1);
	buf2 = ReadBuffer(ctx->indexes[1], blk2);
	LockBuffer(buf1, GIST_SHARE);
	LockBuffer(buf2, GIST_SHARE);
	page1 = BufferGetPage(buf1);
	page2 = BufferGetPage(buf2);

	/* Further processing depends on page types (internal/leaf) */
	if (GistPageIsLeaf(page1) && !GistPageIsLeaf(page2))
	{
		/*
		 * First page is leaf while second one is internal. Generate pending
		 * pairs with same first page and children of second page.
		 */
		//int32		key[6];

		//getPointsBoundKey(ctx, &buf1, parentlsn1, 1, key);
		scanForPendingPages(ctx, &buf2, blk1, 2, parentlsn2, parentlsn1, box1, box2);
	}
	else if (!GistPageIsLeaf(page1) && GistPageIsLeaf(page2))
	{
		/*
		 * First page is internal while second one is leaf. Symmetrical to
		 * previous case.
		 */
		//int32		key[6];

		//getPointsBoundKey(ctx, &buf2, parentlsn2, 2, key);
		scanForPendingPages(ctx, &buf1, blk2, 1, parentlsn1, parentlsn2, box1, box2);
	}
	else if (GistPageIsLeaf(page1) && GistPageIsLeaf(page2))
	{
		/* Both pages are leaf: do line sweep for points */
		PointInfo  *points1,
				   *points2;
		int			pi1,
					pi2;

		MemoryContext tempCtx = AllocSetContextCreate(CurrentMemoryContext,"processPendingPair",ALLOCSET_DEFAULT_SIZES);
		MemoryContext saveContext = MemoryContextSwitchTo(tempCtx);

		points1 = readPoints(ctx, &buf1, parentlsn1, 1, &pi1);
		points2 = readPoints(ctx, &buf2, parentlsn2, 2, &pi2);


		pointLineSweep(ctx, points1, pi1, points2, pi2, saveContext, box1, box2);

		MemoryContextDelete(tempCtx);
	}
	else
	{
		/* Both pages are internal: do line sweep for 3D boxes */
		Box3DInfo  *boxes1,
				   *boxes2;
		int			bi1,
					bi2;

		MemoryContext tempCtx = AllocSetContextCreate(CurrentMemoryContext,"processPendingPair",ALLOCSET_DEFAULT_SIZES);
				MemoryContext saveContext = MemoryContextSwitchTo(tempCtx);

		boxes1 = readBoxes(ctx, &buf1, parentlsn1, 1, &bi1);
		boxes2 = readBoxes(ctx, &buf2, parentlsn2, 2, &bi2);


		box3DLineSweep(ctx, boxes1, bi1, boxes2, bi2, saveContext, box1, box2);
		MemoryContextDelete(tempCtx);
	}

	UnlockReleaseBuffer(buf1);
	UnlockReleaseBuffer(buf2);
}


void
crossmatch(CrossmatchContext *ctx, ItemPointer values)
{
	/* Scan pending pairs until we have some result pairs */
	while (ctx->resultsPairs.count == 0 && ctx->pendingPairs.count)
	{
		PendingPair blockNumberPair;


		blockNumberPair = stackPop(&ctx->pendingPairs);


		processPendingPair(ctx, blockNumberPair.blk1, blockNumberPair.blk2,
					 blockNumberPair.parentlsn1, blockNumberPair.parentlsn2,blockNumberPair.box1,blockNumberPair.box2);
	}

	/* Return next result pair if any. Otherwise close SRF. */
	if (ctx->resultsPairs.count != 0)
	{
		ResultPair itemPointerPair = dequeue(&ctx->resultsPairs);


		values[0] = itemPointerPair.iptr1;
		values[1] = itemPointerPair.iptr2;
	}
	else
	{
		ItemPointerSetInvalid(&values[0]);
		ItemPointerSetInvalid(&values[1]);
	}

}
