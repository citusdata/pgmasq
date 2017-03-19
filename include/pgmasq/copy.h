#pragma once

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"


/*
 * A smaller version of copy.c's CopyStateData, trimmed to the elements
 * necessary to copy out results. While it'd be a bit nicer to share code,
 * it'd require changing core postgres code.
 */
typedef struct CopyOutStateData
{
	StringInfo fe_msgbuf;       /* used for all dests during COPY TO, only for
	                             * dest == COPY_NEW_FE in COPY FROM */
	int file_encoding;          /* file or remote side's character encoding */
	bool need_transcoding;              /* file encoding diff from server? */
	bool binary;                /* binary format? */
	char *null_print;           /* NULL marker string (server encoding!) */
	char *null_print_client;            /* same converted to file encoding */
	char *delim;                /* column delimiter (must be 1 byte) */

	MemoryContext rowcontext;   /* per-row evaluation context */
} CopyOutStateData;

typedef struct CopyOutStateData *CopyOutState;

extern void PgMasqCopyFrom(CopyStmt *copyStatement, char *completionTag);
