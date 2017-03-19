/*-------------------------------------------------------------------------
 *
 * src/pgmasq.c
 *
 * This file contains functions to forward queries to another postgres
 * server.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include <arpa/inet.h> /* for htons */
#include <netinet/in.h> /* for htons */
#include <string.h>

#include "access/htup_details.h"
#include "access/htup.h"
#include "access/sdir.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "pgmasq/copy.h"
#include "pgmasq/execute_upstream.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/memutils.h"


/* constant used in binary protocol */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";


/*
 * Represents the different source/dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_FILE,					/* to/from file (or a piped program) */
	COPY_OLD_FE,				/* to/from frontend (2.0 protocol) */
	COPY_NEW_FE					/* to/from frontend (3.0 protocol) */
} CopyDest;

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL
} EolType;

/*
 * This struct contains all the state variables used throughout a COPY
 * operation. For simplicity, we use the same struct for all variants of COPY,
 * even though some fields are used in only some cases.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is TRUE
 * when we have to do it the hard way.
 */
typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool		fe_eof;			/* true if detected end of copy data */
	EolType		eol_type;		/* EOL type of input */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;		/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to or from */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDIN/STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */
	bool		binary;			/* binary format? */
	bool		oids;			/* include OIDs? */
	bool		freeze;			/* freeze rows on loading? */
	bool		csv_mode;		/* Comma Separated Value format? */
	bool		header_line;	/* CSV header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;		/* same converted to file encoding */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE_QUOTE *? */
	bool	   *force_quote_flags;		/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool	   *force_null_flags;		/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	List	   *convert_select; /* list of column names (can be NIL) */
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	int			cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */

	/*
	 * Working state for COPY FROM
	 */
	AttrNumber	num_defaults;
	bool		file_has_oids;
	FmgrInfo	oid_in_function;
	Oid			oid_typioparam;
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	int		   *defmap;			/* array of default att numbers */
	ExprState **defexprs;		/* array of default att expressions */
	bool		volatile_defexprs;		/* is any of defexprs volatile? */
	List	   *range_table;

	/*
	 * These variables are used to reduce overhead in textual COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int			max_fields;
	char	  **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.
	 */
	StringInfoData line_buf;
	bool		line_buf_converted;		/* converted to server encoding? */
	bool		line_buf_valid; /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).  CopyReadLine parses this data sufficiently to
	 * locate line boundaries, then transfers the data to line_buf and
	 * converts it.  Note: we guarantee that there is a \0 at
	 * raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */
} CopyStateData;


/* Local functions forward declarations */
static bool CanUseBinaryCopyFormat(TupleDesc tupleDescription,
								   CopyOutState rowOutputState);
static FmgrInfo * ColumnOutputFunctions(TupleDesc rowDescriptor, bool binaryFormat);
static StringInfo ConstructCopyStatement(CopyStmt *copyStatement,
										 bool useBinaryCopyFormat);
static void SendCopyBinaryHeaders(CopyOutState copyOutState, PGconn *connection);
static void SendCopyBinaryFooters(CopyOutState copyOutState, PGconn *connection);
static void SendCopyData(StringInfo dataBuffer, PGconn *connection);
static void EndRemoteCopy(PGconn *connection);
static uint32 AvailableColumnCount(TupleDesc tupleDescriptor, bool* columnIncluded);

/* Private functions copied and adapted from copy.c in PostgreSQL */
static void AppendCopyRowData(Datum *valueArray, bool *isNullArray,
							  TupleDesc rowDescriptor, CopyOutState rowOutputState,
							  FmgrInfo *columnOutputFunctions, bool *columnIncluded);
static void AppendCopyBinaryHeaders(CopyOutState headerOutputState);
static void AppendCopyBinaryFooters(CopyOutState headerOutputState);
static void CopySendData(CopyOutState outputState, const void *databuf, int datasize);
static void CopySendString(CopyOutState outputState, const char *str);
static void CopySendChar(CopyOutState outputState, char c);
static void CopySendInt32(CopyOutState outputState, int32 val);
static void CopySendInt16(CopyOutState outputState, int16 val);
static void CopyAttributeOutText(CopyOutState outputState, char *string);
static inline void CopyFlushOutput(CopyOutState outputState, char *start, char *pointer);


/*
 * PgMasqCopyFrom implements COPY table_name FROM .. by forwarding
 * rows upstream.
 */
void
PgMasqCopyFrom(CopyStmt *copyStatement, char *completionTag)
{
	Oid relationId = RangeVarGetRelid(copyStatement->relation, NoLock, false);
	Relation relation = heap_open(relationId, RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	uint32 columnCount = tupleDescriptor->natts;
	int columnIndex = 0;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));
	bool *columnIncluded = palloc0(columnCount * sizeof(bool));
	uint64 processedRowCount = 0;

	CopyState copyState = NULL;
	CopyOutState copyOutState = NULL;
	StringInfo copyCommand = NULL;

	EState *executorState = CreateExecutorState();
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	ExprContext *executorExpressionContext = GetPerTupleExprContext(executorState);

	FmgrInfo *columnOutputFunctions = NULL;
	const char *delimiterCharacter = "\t";
	const char *nullPrintCharacter = "\\N";

	ErrorContextCallback errorCallback;

	PGconn *connection = OpenUpstreamConnection();
	PGresult *result = NULL;

	if (copyStatement->filename != NULL && !superuser())
	{
		if (copyStatement->is_program)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from an external program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a file"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
	}

	copyState = BeginCopyFrom(relation,
							  copyStatement->filename,
							  copyStatement->is_program,
							  copyStatement->attlist,
							  copyStatement->options);

	/* prevent nextval calls */
	copyState->num_defaults = 0;

	/* exclude columns that are not in attlist from the result */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		int attnum = columnIndex + 1;

		if (list_member_int(copyState->attnumlist, attnum))
		{
			columnIncluded[columnIndex] = true;
		}
		else
		{
			columnIncluded[columnIndex] = false;
		}
	}

	copyOutState = (CopyOutState) palloc0(sizeof(CopyOutStateData));
	copyOutState->delim = (char *) delimiterCharacter;
	copyOutState->null_print = (char *) nullPrintCharacter;
	copyOutState->null_print_client = (char *) nullPrintCharacter;
	copyOutState->binary = CanUseBinaryCopyFormat(tupleDescriptor, copyOutState);
	copyOutState->binary = false;
	copyOutState->fe_msgbuf = makeStringInfo();
	copyOutState->rowcontext = executorTupleContext;

	columnOutputFunctions = ColumnOutputFunctions(tupleDescriptor, copyOutState->binary);

	copyCommand = ConstructCopyStatement(copyStatement, copyOutState->binary);

	connection = OpenUpstreamConnection();

	elog(DEBUG4, "pgmasq sending upstream: %s", copyCommand->data);

	SendCommandUpstream(connection, copyCommand->data, NULL);

	result = ReceiveUpstreamCommandResult(connection);
	if (PQresultStatus(result) != PGRES_COPY_IN)
	{
		PgMasqReportError(ERROR, result, connection, false);
	}

	PQclear(result);

	if (copyOutState->binary)
	{
		SendCopyBinaryHeaders(copyOutState, connection);
	}

	/* set up callback to identify error line number */
	errorCallback.callback = CopyFromErrorCallback;
	errorCallback.arg = (void *) copyState;
	errorCallback.previous = error_context_stack;

	while (true)
	{
		bool nextRowFound = false;
		MemoryContext oldContext = NULL;

		ResetPerTupleExprContext(executorState);

		/* switch to tuple memory context and start showing line number in errors */
		error_context_stack = &errorCallback;
		oldContext = MemoryContextSwitchTo(executorTupleContext);

		/* parse a row from the input */
		nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
									columnValues, columnNulls, NULL);

		if (!nextRowFound)
		{
			/* switch to regular memory context and stop showing line number in errors */
			MemoryContextSwitchTo(oldContext);
			error_context_stack = errorCallback.previous;
			break;
		}

		CHECK_FOR_INTERRUPTS();

		/* switch to regular memory context and stop showing line number in errors */
		MemoryContextSwitchTo(oldContext);
		error_context_stack = errorCallback.previous;

		/* serialised and send a row */
		resetStringInfo(copyOutState->fe_msgbuf);
		AppendCopyRowData(columnValues, columnNulls, tupleDescriptor,
						  copyOutState, columnOutputFunctions,
						  columnIncluded);
		SendCopyData(copyOutState->fe_msgbuf, connection);

		processedRowCount += 1;
	}

	if (copyOutState->binary)
	{
		SendCopyBinaryFooters(copyOutState, connection);
	}

	EndRemoteCopy(connection);
	EndCopyFrom(copyState);
	heap_close(relation, NoLock);

	/* check for cancellation one last time before returning */
	CHECK_FOR_INTERRUPTS();

	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processedRowCount);
	}
}


/*
 * CanUseBinaryCopyFormat iterates over columns of the relation given in rowOutputState
 * and looks for a column whose type is array of user-defined type or composite type.
 * If it finds such column, that means we cannot use binary format for COPY, because
 * binary format sends Oid of the types, which are generally not same in master and
 * worker nodes for user-defined types.
 */
static bool
CanUseBinaryCopyFormat(TupleDesc tupleDescription, CopyOutState rowOutputState)
{
	bool useBinaryCopyFormat = true;
	int totalColumnCount = tupleDescription->natts;
	int columnIndex = 0;

	for (columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescription->attrs[columnIndex];
		Oid typeId = InvalidOid;
		char typeCategory = '\0';
		bool typePreferred = false;

		if (currentColumn->attisdropped)
		{
			continue;
		}

		typeId = currentColumn->atttypid;
		if (typeId >= FirstNormalObjectId)
		{
			get_type_category_preferred(typeId, &typeCategory, &typePreferred);
			if (typeCategory == TYPCATEGORY_ARRAY ||
				typeCategory == TYPCATEGORY_COMPOSITE)
			{
				useBinaryCopyFormat = false;
				break;
			}
		}
	}

	return useBinaryCopyFormat;
}


/* Send copy binary headers to given connections */
static void
SendCopyBinaryHeaders(CopyOutState copyOutState, PGconn *connection)
{
	resetStringInfo(copyOutState->fe_msgbuf);
	AppendCopyBinaryHeaders(copyOutState);
	SendCopyData(copyOutState->fe_msgbuf, connection);
}


/* Send copy binary footers to given connections */
static void
SendCopyBinaryFooters(CopyOutState copyOutState, PGconn *connection)
{
	resetStringInfo(copyOutState->fe_msgbuf);
	AppendCopyBinaryFooters(copyOutState);
	SendCopyData(copyOutState->fe_msgbuf, connection);
}


/*
 * ConstructCopyStatement constructs the text of a COPY statement
 * to be executed on the primary.
 */
static StringInfo
ConstructCopyStatement(CopyStmt *copyStatement, bool useBinaryCopyFormat)
{
	StringInfo command = makeStringInfo();

	char *schemaName = copyStatement->relation->schemaname;
	char *relationName = copyStatement->relation->relname;
	char *qualifiedRelationName = NULL;

	qualifiedRelationName = quote_qualified_identifier(schemaName, relationName);

	appendStringInfo(command, "COPY %s ", qualifiedRelationName);

	if (copyStatement->attlist != NIL)
	{
		ListCell *columnNameCell = NULL;
		bool appendedFirstName = false;

		foreach(columnNameCell, copyStatement->attlist)
		{
			Value *columnNameValue = (Value *) lfirst(columnNameCell);
			char *columnName = strVal(columnNameValue);

			if (!appendedFirstName)
			{
				appendStringInfo(command, "(%s", columnName);
				appendedFirstName = true;
			}
			else
			{
				appendStringInfo(command, ", %s", columnName);
			}
		}

		appendStringInfoString(command, ") ");
	}

	appendStringInfo(command, "FROM STDIN WITH ");

	if (useBinaryCopyFormat)
	{
		appendStringInfoString(command, "(FORMAT BINARY)");
	}
	else
	{
		appendStringInfoString(command, "(FORMAT TEXT)");
	}

	return command;
}


/*
 * SendCopyData sends serialized COPY data over the given connection.
 */
static void
SendCopyData(StringInfo dataBuffer, PGconn *connection)
{
	int copyResult = PQputCopyData(connection, dataBuffer->data, dataBuffer->len);
	if (copyResult != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to COPY to primary")));
	}
}


/*
 * EndRemoteCopy ends the COPY input on the given connection.
 */
static void
EndRemoteCopy(PGconn *connection)
{
	int copyEndResult = 0;
	PGresult *result = NULL;

	/* end the COPY input */
	copyEndResult = PQputCopyEnd(connection, NULL);
	if (copyEndResult != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("failed to COPY to primary")));
	}

	/* check whether there were any COPY errors */
	result = PQgetResult(connection);
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		PgMasqReportError(ERROR, result, connection, false);
	}

	ClearResults(connection);
}


/*
 * ColumnOutputFunctions walks over a table's columns, and finds each column's
 * type information. The function then resolves each type's output function,
 * and stores and returns these output functions in an array.
 */
static FmgrInfo *
ColumnOutputFunctions(TupleDesc rowDescriptor, bool binaryFormat)
{
	uint32 columnCount = (uint32) rowDescriptor->natts;
	FmgrInfo *columnOutputFunctions = palloc0(columnCount * sizeof(FmgrInfo));

	uint32 columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *currentOutputFunction = &columnOutputFunctions[columnIndex];
		Form_pg_attribute currentColumn = rowDescriptor->attrs[columnIndex];
		Oid columnTypeId = currentColumn->atttypid;
		Oid outputFunctionId = InvalidOid;
		bool typeVariableLength = false;

		if (currentColumn->attisdropped)
		{
			/* dropped column, leave the output function NULL */
			continue;
		}
		else if (binaryFormat)
		{
			getTypeBinaryOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}
		else
		{
			getTypeOutputInfo(columnTypeId, &outputFunctionId, &typeVariableLength);
		}

		fmgr_info(outputFunctionId, currentOutputFunction);
	}

	return columnOutputFunctions;
}


/*
 * AppendCopyRowData serializes one row using the column output functions,
 * and appends the data to the row output state object's message buffer.
 * This function is modeled after the CopyOneRowTo() function in
 * commands/copy.c, but only implements a subset of that functionality.
 * Note that the caller of this function should reset row memory context
 * to not bloat memory usage.
 */
void
AppendCopyRowData(Datum *valueArray, bool *isNullArray, TupleDesc rowDescriptor,
				  CopyOutState rowOutputState, FmgrInfo *columnOutputFunctions,
				  bool *columnIncluded)
{
	uint32 totalColumnCount = (uint32) rowDescriptor->natts;
	uint32 availableColumnCount = AvailableColumnCount(rowDescriptor, columnIncluded);
	uint32 appendedColumnCount = 0;
	uint32 columnIndex = 0;

	MemoryContext oldContext = MemoryContextSwitchTo(rowOutputState->rowcontext);

	if (rowOutputState->binary)
	{
		CopySendInt16(rowOutputState, availableColumnCount);
	}
	for (columnIndex = 0; columnIndex < totalColumnCount; columnIndex++)
	{
		Form_pg_attribute currentColumn = rowDescriptor->attrs[columnIndex];
		Datum value = valueArray[columnIndex];
		bool isNull = isNullArray[columnIndex];
		bool lastColumn = false;

		if (currentColumn->attisdropped || !columnIncluded[columnIndex])
		{
			continue;
		}
		else if (rowOutputState->binary)
		{
			if (!isNull)
			{
				FmgrInfo *outputFunctionPointer = &columnOutputFunctions[columnIndex];
				bytea *outputBytes = SendFunctionCall(outputFunctionPointer, value);

				CopySendInt32(rowOutputState, VARSIZE(outputBytes) - VARHDRSZ);
				CopySendData(rowOutputState, VARDATA(outputBytes),
							 VARSIZE(outputBytes) - VARHDRSZ);
			}
			else
			{
				CopySendInt32(rowOutputState, -1);
			}
		}
		else
		{
			if (!isNull)
			{
				FmgrInfo *outputFunctionPointer = &columnOutputFunctions[columnIndex];
				char *columnText = OutputFunctionCall(outputFunctionPointer, value);

				CopyAttributeOutText(rowOutputState, columnText);
			}
			else
			{
				CopySendString(rowOutputState, rowOutputState->null_print_client);
			}

			lastColumn = ((appendedColumnCount + 1) == availableColumnCount);
			if (!lastColumn)
			{
				CopySendChar(rowOutputState, rowOutputState->delim[0]);
			}
		}

		appendedColumnCount++;
	}

	if (!rowOutputState->binary)
	{
		/* append default line termination string depending on the platform */
#ifndef WIN32
		CopySendChar(rowOutputState, '\n');
#else
		CopySendString(rowOutputState, "\r\n");
#endif
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * AvailableColumnCount returns the number of columns in a tuple descriptor, excluding
 * columns that were dropped.
 */
static uint32
AvailableColumnCount(TupleDesc tupleDescriptor, bool *columnIncluded)
{
	uint32 columnCount = 0;
	uint32 columnIndex = 0;

	for (columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescriptor->attrs[columnIndex];

		if (!currentColumn->attisdropped && columnIncluded[columnIndex])
		{
			columnCount++;
		}
	}

	return columnCount;
}


/*
 * AppendCopyBinaryHeaders appends binary headers to the copy buffer in
 * headerOutputState.
 */
static void
AppendCopyBinaryHeaders(CopyOutState headerOutputState)
{
	const int32 zero = 0;
	MemoryContext oldContext = MemoryContextSwitchTo(headerOutputState->rowcontext);

	/* Signature */
	CopySendData(headerOutputState, BinarySignature, 11);

	/* Flags field (no OIDs) */
	CopySendInt32(headerOutputState, zero);

	/* No header extension */
	CopySendInt32(headerOutputState, zero);

	MemoryContextSwitchTo(oldContext);
}


/*
 * AppendCopyBinaryFooters appends binary footers to the copy buffer in
 * footerOutputState.
 */
static void
AppendCopyBinaryFooters(CopyOutState footerOutputState)
{
	int16 negative = -1;
	MemoryContext oldContext = MemoryContextSwitchTo(footerOutputState->rowcontext);

	CopySendInt16(footerOutputState, negative);

	MemoryContextSwitchTo(oldContext);
}


/* *INDENT-OFF* */
/* Append data to the copy buffer in outputState */
static void
CopySendData(CopyOutState outputState, const void *databuf, int datasize)
{
	appendBinaryStringInfo(outputState->fe_msgbuf, databuf, datasize);
}


/* Append a striong to the copy buffer in outputState. */
static void
CopySendString(CopyOutState outputState, const char *str)
{
	appendBinaryStringInfo(outputState->fe_msgbuf, str, strlen(str));
}


/* Append a char to the copy buffer in outputState. */
static void
CopySendChar(CopyOutState outputState, char c)
{
	appendStringInfoCharMacro(outputState->fe_msgbuf, c);
}


/* Append an int32 to the copy buffer in outputState. */
static void
CopySendInt32(CopyOutState outputState, int32 val)
{
	uint32 buf = htonl((uint32) val);
	CopySendData(outputState, &buf, sizeof(buf));
}


/* Append an int16 to the copy buffer in outputState. */
static void
CopySendInt16(CopyOutState outputState, int16 val)
{
	uint16 buf = htons((uint16) val);
	CopySendData(outputState, &buf, sizeof(buf));
}


/*
 * Send text representation of one column, with conversion and escaping.
 *
 * NB: This function is based on commands/copy.c and doesn't fully conform to
 * our coding style. The function should be kept in sync with copy.c.
 */
static void
CopyAttributeOutText(CopyOutState cstate, char *string)
{
	char *pointer = NULL;
	char *start = NULL;
	char c = '\0';
	char delimc = cstate->delim[0];

	if (cstate->need_transcoding)
	{
		pointer = pg_server_to_any(string, strlen(string), cstate->file_encoding);
	}
	else
	{
		pointer = string;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "pointer"
	 * can be sent literally, but hasn't yet been.
	 *
	 * As all encodings here are safe, i.e. backend supported ones, we can
	 * skip doing pg_encoding_mblen(), because in valid backend encodings,
	 * extra bytes of a multibyte character never look like ASCII.
	 */
	start = pointer;
	while ((c = *pointer) != '\0')
	{
		if ((unsigned char) c < (unsigned char) 0x20)
		{
			/*
			 * \r and \n must be escaped, the others are traditional. We
			 * prefer to dump these using the C-like notation, rather than
			 * a backslash and the literal character, because it makes the
			 * dump file a bit more proof against Microsoftish data
			 * mangling.
			 */
			switch (c)
			{
				case '\b':
					c = 'b';
					break;
				case '\f':
					c = 'f';
					break;
				case '\n':
					c = 'n';
					break;
				case '\r':
					c = 'r';
					break;
				case '\t':
					c = 't';
					break;
				case '\v':
					c = 'v';
					break;
				default:
					/* If it's the delimiter, must backslash it */
					if (c == delimc)
						break;
					/* All ASCII control chars are length 1 */
					pointer++;
					continue;		/* fall to end of loop */
			}
			/* if we get here, we need to convert the control char */
			CopyFlushOutput(cstate, start, pointer);
			CopySendChar(cstate, '\\');
			CopySendChar(cstate, c);
			start = ++pointer;	/* do not include char in next run */
		}
		else if (c == '\\' || c == delimc)
		{
			CopyFlushOutput(cstate, start, pointer);
			CopySendChar(cstate, '\\');
			start = pointer++;	/* we include char in next run */
		}
		else
		{
			pointer++;
		}
	}

	CopyFlushOutput(cstate, start, pointer);
}


/* *INDENT-ON* */
/* Helper function to send pending copy output */
static inline void
CopyFlushOutput(CopyOutState cstate, char *start, char *pointer)
{
	if (pointer > start)
	{
		CopySendData(cstate, start, pointer - start);
	}
}
