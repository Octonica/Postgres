#ifndef _SPATIALJOIN_H_
#define _SPATIALJOIN_H_
#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/gist.h"
#include "access/stratnum.h"
#include "utils/array.h"
#include "utils/builtins.h"
#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif
#include "catalog/namespace.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= 90600
#define heap_formtuple heap_form_tuple
#endif

#include "cubedata.h"

#if PG_VERSION_NUM >= 90300
#define GIST_SCAN_FOLLOW_RIGHT(parentlsn,page) \
	(!XLogRecPtrIsInvalid(parentlsn) && \
	(GistFollowRight(page) || \
	parentlsn < GistPageGetNSN(page)) && \
	GistPageGetOpaque(page)->rightlink != InvalidBlockNumber /* sanity check */ )
#define InvalidNSN 0
#else
#define GIST_SCAN_FOLLOW_RIGHT(parentlsn,page) \
	(!XLogRecPtrIsInvalid(parentlsn) && \
	(GistFollowRight(page) || \
	XLByteLT(parentlsn, GistPageGetOpaque(page)->nsn)) && \
	GistPageGetOpaque(page)->rightlink != InvalidBlockNumber /* sanity check */ )
#define InvalidNSN {0, 0}
#endif

/*
 * Pair of pages for pending scan.
 */
typedef struct
{
	GistNSN		parentlsn1,
				parentlsn2;
	BlockNumber blk1,
				blk2;
} PendingPair;


/*
 * Resulting pair of item pointer for return by CRF.
 */
typedef struct
{
	ItemPointerData iptr1,
				iptr2;
} ResultPair;

#define QueueSegmentSize 256

typedef struct QueueSegment
{
	ResultPair Data[QueueSegmentSize];
	struct QueueSegment* NextSegment;
} QueueSegment;

typedef struct
{
	int head_index;
	int tail_index;
	int count;
	QueueSegment* head;
	QueueSegment* tail;
	QueueSegment* backlog;
} Queue;




typedef struct StackSegment
{
	PendingPair Data[QueueSegmentSize];
	struct StackSegment* NextSegment;
} StackSegment;

typedef struct
{
	int head_index;
	int count;
	StackSegment* head;
	StackSegment* backlog;
} Stack;

/*
 * Context of crossmatch: represents data which are persistent across SRF calls.
 */
typedef struct CrossmatchContext
{
	MemoryContext context;
	Relation	indexes[2];
	Stack	    pendingPairs;
	Queue	    resultsPairs;
	NDBOX	   *box;
} CrossmatchContext;

typedef struct
{
	NDBOX*		cube;
	ItemPointerData iptr;
} PointInfo;

typedef struct
{
	NDBOX*		cube;
	BlockNumber blk;
	GistNSN		parentlsn;
} Box3DInfo;

extern void setupFirstcallNode(CrossmatchContext *ctx, Oid idx1, Oid idx2);
extern void endCall(CrossmatchContext *ctx);
extern void crossmatch(CrossmatchContext *ctx, ItemPointer values);

#endif

