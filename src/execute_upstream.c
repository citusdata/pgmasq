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
#include "poll.h"

#include <stddef.h>
#include <string.h>

#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "executor/execdesc.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "pgmasq/conninfo.h"
#include "pgmasq/execute_upstream.h"
#include "pgmasq/pgmasq.h"
#include "pgmasq/ruleutils.h"
#include "replication/walreceiver.h"
#include "storage/latch.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"


/* logs each statement used in a distributed plan */
char *ConnectionInfoOverride = NULL;

/* current connection, if any */
static PGconn *UpstreamConnection = NULL;
static bool ConnectAllowed = true;


/* forward declarations */
static void ReceiveNotice(void *arg, const PGresult *result);
static void UseCurrentRoleUpstream(PGconn *connection);
static void ExtractParametersFromParamListInfo(ParamListInfo paramListInfo,
											   Oid **parameterTypes,
											   const char ***parameterValues);
static void ForwardCommandResultToClient(PGconn *connection, EState *executorState,
										 char *completionTag);
static TupleDesc TupleDescriptorForResult(PGresult *result);
static void ForwardTuplesToClient(PGconn *connection, EState *executorState,
								  DestReceiver *destination);


/*
 * PgMasqQuery performs a query remotely and displays the results.
 */
void
PgMasqQuery(const char *queryString, ParamListInfo params, EState *estate,
			bool sendTuples, DestReceiver *dest, char *completionTag)
{
	PGconn *connection = NULL;

	connection = OpenUpstreamConnection();

	SendCommandUpstream(connection, queryString, params);

	if (sendTuples)
	{
		ForwardTuplesToClient(connection, estate, dest);
	}
	else
	{
		ForwardCommandResultToClient(connection, estate, completionTag);
	}
}


/*
 * ExecuteSimpleQueryUpstream executes a query upstream and gets
 * the results as a list of strings.
 */
List *
ExecuteSimpleQueryUpstream(const char *queryString)
{
	PGconn *connection = NULL;
	PGresult *result = NULL;
	List *rowValues = NIL;

	connection = OpenUpstreamConnection();

	SendCommandUpstream(connection, queryString, NULL);

	while ((result = ReceiveUpstreamCommandResult(connection)) != NULL)
	{
		uint32 rowCount = 0;
		uint32 rowIndex = 0;
		char *rowValue = NULL;

		ExecStatusType resultStatus = PQresultStatus(result);
		if (resultStatus != PGRES_TUPLES_OK && resultStatus != PGRES_SINGLE_TUPLE)
		{
			PgMasqReportError(ERROR, result, connection, true);
		}

		rowCount = PQntuples(result);

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			if (PQgetisnull(result, rowIndex, 0))
			{
				rowValue = "";
			}
			else
			{
				rowValue = PQgetvalue(result, rowIndex, 0);
			}

			rowValues = lappend(rowValues, pstrdup(rowValue));
		}

		PQclear(result);
	}

	return rowValues;
}


/*
 * OpenUpstreamConnection returns a connection to the upstream node.
 */
PGconn *
OpenUpstreamConnection(void)
{
	static int checkIntervalMS = 200;
	TimestampTz connectionStart = 0;
	StringInfo connectionInfo = NULL;
	PGconn *connection = NULL;

	if (UpstreamConnection != NULL)
	{
		return UpstreamConnection;
	}

	if (!ConnectAllowed)
	{
		/*
		 * The user sent a SET command, but then we lost connection and we
		 * cannot ensure the GUCs have the right values after a reconnect.
		 * We kill the session to allow the application to start a new
		 * session and recover, rather than throwing errors.
		 */
		ereport(FATAL, (errmsg("cannot reconnect to upstream")));
	}

	connectionInfo = makeStringInfo();

	if (ConnectionInfoOverride != NULL)
	{
		appendStringInfoString(connectionInfo, ConnectionInfoOverride);
	}
	else
	{
		char *primaryConnectionInfo = ReadPrimaryConnInfoFromRecoveryConf();
		appendStringInfoString(connectionInfo, primaryConnectionInfo);
	}

	/* we're explicitly connected to a database */
	appendStringInfo(connectionInfo, " dbname=%s", get_database_name(MyDatabaseId));

	elog(DEBUG4, "pgmasq connecting to: %s", connectionInfo->data);

	connectionStart = GetCurrentTimestamp();
	connection = PQconnectStart(connectionInfo->data);
	if (connection == NULL)
	{
		ereport(ERROR, (errmsg("cannot connect to %s", connectionInfo->data)));
	}

	/*
	 * Loop until connection is established, or failed (possibly just timed
	 * out).
	 */
	while (true)
	{
		ConnStatusType status = PQstatus(connection);
		PostgresPollingStatusType pollmode;

		if (status == CONNECTION_OK)
		{
			break;
		}

		if (status == CONNECTION_BAD)
		{
			PgMasqReportError(ERROR, NULL, connection, true);
		}

		pollmode = PQconnectPoll(connection);

		/*
		 * FIXME: Do we want to add transparent retry support here?
		 */
		if (pollmode == PGRES_POLLING_FAILED)
		{
			PgMasqReportError(ERROR, NULL, connection, true);
		}
		else if (pollmode == PGRES_POLLING_OK)
		{
			break;
		}
		else
		{
			Assert(pollmode == PGRES_POLLING_WRITING ||
				   pollmode == PGRES_POLLING_READING);
		}

		/* Loop, to handle poll() being interrupted by signals (EINTR) */
		while (true)
		{
			struct pollfd pollFileDescriptor;
			int pollResult = 0;

			pollFileDescriptor.fd = PQsocket(connection);
			if (pollmode == PGRES_POLLING_READING)
			{
				pollFileDescriptor.events = POLLIN;
			}
			else
			{
				pollFileDescriptor.events = POLLOUT;
			}
			pollFileDescriptor.revents = 0;

			/*
			 * Only sleep for a limited amount of time, so we can react to
			 * interrupts in time, even if the platform doesn't interrupt
			 * poll() after signal arrival.
			 */
			pollResult = poll(&pollFileDescriptor, 1, checkIntervalMS);

			if (pollResult == 0)
			{
				/*
				 * Timeout exceeded. Two things to do:
				 * - check whether any interrupts arrived and handle them
				 * - check whether establishment for connection already has
				 *   lasted for too long, stop waiting if so.
				 */
				CHECK_FOR_INTERRUPTS();

				if (TimestampDifferenceExceeds(connectionStart,
											   GetCurrentTimestamp(),
											   4000))
				{
					ereport(ERROR, (errmsg("could not establish connection after %u ms",
										   4000)));
				}
			}
			else if (pollResult > 0)
			{
				/*
				 * IO possible, continue connection establishment. We could
				 * check for timeouts here as well, but if there's progress
				 * there seems little point.
				 */
				break;
			}
			else if (pollResult != EINTR)
			{
				/* Retrying, signal interrupted. So check. */
				CHECK_FOR_INTERRUPTS();
			}
			else
			{
				/*
				 * We here, instead of just returning a failed
				 * connection, because this shouldn't happen, and indicates a
				 * programming error somewhere, not a network etc. issue.
				 */
				ereport(ERROR, (errcode_for_socket_access(),
								errmsg("poll() failed: %m")));
			}
		}
	}

	PQsetNoticeReceiver(connection, ReceiveNotice, NULL);

	UseCurrentRoleUpstream(connection);

	/* connection successfully established, cache it */
	UpstreamConnection = connection;

	return connection;
}


/*
 * ReceiveNotice is called when a notice or warning is received.
 */
static void
ReceiveNotice(void *arg, const PGresult *result)
{
	char *severity = PQresultErrorField(result, PG_DIAG_SEVERITY_NONLOCALIZED);
	int errorLevel = 0;

	if (strcmp(severity, "DEBUG") == 0)
	{
		errorLevel = DEBUG1;
	}
	else if (strcmp(severity, "LOG") == 0)
	{
		errorLevel = LOG;
	}
	else if (strcmp(severity, "INFO") == 0)
	{
		errorLevel = INFO;
	}
	else if (strcmp(severity, "NOTICE") == 0)
	{
		errorLevel = NOTICE;
	}
	else if (strcmp(severity, "WARNING") == 0)
	{
		errorLevel = WARNING;
	}

	if (errorLevel > 0)
	{
		PgMasqReportError(errorLevel, (PGresult *) result, NULL, false);
	}
}


/*
 * UseCurrentRoleUpstream ensures that the upstream connection uses the same
 * role as the current connection by reauthenticating via the
 * pgmasq.reauthenticate function.
 */
static void
UseCurrentRoleUpstream(PGconn *connection)
{
	Oid userId = GetUserId();
	char *currentRole = GetUserNameFromId(userId, false);
	char *quotedRole = quote_literal_cstr(currentRole);
	PGresult *result = NULL;

	StringInfo reauthenticateQuery = makeStringInfo();

	appendStringInfo(reauthenticateQuery, "SELECT pgmasq.reauthenticate(%s)", quotedRole);

	SendCommandUpstream(connection, reauthenticateQuery->data, NULL);

	while ((result = ReceiveUpstreamCommandResult(connection)) != NULL)
	{
		ExecStatusType resultStatus = PQresultStatus(result);
		if (resultStatus != PGRES_TUPLES_OK && resultStatus != PGRES_SINGLE_TUPLE)
		{
			PgMasqReportError(ERROR, result, connection, false);
		}

		PQclear(result);
	}
}


/*
 * SendCommandUpstream sends a command over the given connection.
 */
void
SendCommandUpstream(PGconn *connection, const char *command, ParamListInfo paramListInfo)
{
	int parameterCount = 0;
	Oid *parameterTypes = NULL;
	const char **parameterValues = NULL;
	bool wasNonblocking = false;
	int singleRowMode PG_USED_FOR_ASSERTS_ONLY = 0;
	int querySent = 0;

	Assert(connection != NULL);

	/* include parameters when forwarding command */
	if (paramListInfo != NULL)
	{
		parameterCount = paramListInfo->numParams;

		ExtractParametersFromParamListInfo(paramListInfo, &parameterTypes,
										   &parameterValues);
	}

	wasNonblocking = PQisnonblocking(connection);

	/* make sure not to block anywhere */
	if (!wasNonblocking)
	{
		PQsetnonblocking(connection, true);
	}

	if (parameterCount > 0)
	{
		querySent = PQsendQueryParams(connection, command, parameterCount, parameterTypes,
									  parameterValues, NULL, NULL, 0);
	}
	else
	{
		querySent = PQsendQuery(connection, command);
	}

	/* reset nonblocking connection to its original state */
	if (!wasNonblocking)
	{
		PQsetnonblocking(connection, false);
	}

	if (!querySent)
	{
		PgMasqReportError(ERROR, NULL, connection, true);
	}

	singleRowMode = PQsetSingleRowMode(connection);
	Assert(singleRowMode == 1);
}


/*
 * ExtractParametersFromParamListInfo extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 */
static void
ExtractParametersFromParamListInfo(ParamListInfo paramListInfo, Oid **parameterTypes,
								   const char ***parameterValues)
{
	int parameterIndex = 0;
	int parameterCount = paramListInfo->numParams;

	*parameterTypes = (Oid *) palloc0(parameterCount * sizeof(Oid));
	*parameterValues = (const char **) palloc0(parameterCount * sizeof(char *));

	/* get parameter types and values */
	for (parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++)
	{
		ParamExternData *parameterData = &paramListInfo->params[parameterIndex];
		Oid typeOutputFunctionId = InvalidOid;
		bool variableLengthType = false;

		/*
		 * Use 0 for data types where the oid values can be different on
		 * the master and worker nodes. Therefore, the worker nodes can
		 * infer the correct oid.
		 */
		if (parameterData->ptype >= FirstNormalObjectId)
		{
			(*parameterTypes)[parameterIndex] = 0;
		}
		else
		{
			(*parameterTypes)[parameterIndex] = parameterData->ptype;
		}

		/*
		 * If the parameter is not referenced / used (ptype == 0) and
		 * would otherwise have errored out inside standard_planner()),
		 * don't pass a value to the remote side, and pass text oid to prevent
		 * undetermined data type errors on workers.
		 */
		if (parameterData->ptype == 0)
		{
			(*parameterValues)[parameterIndex] = NULL;
			(*parameterTypes)[parameterIndex] = TEXTOID;

			continue;
		}

		/*
		 * If the parameter is NULL then we preserve its type, but
		 * don't need to evaluate its value.
		 */
		if (parameterData->isnull)
		{
			(*parameterValues)[parameterIndex] = NULL;

			continue;
		}

		getTypeOutputInfo(parameterData->ptype, &typeOutputFunctionId,
						  &variableLengthType);

		(*parameterValues)[parameterIndex] = OidOutputFunctionCall(typeOutputFunctionId,
																   parameterData->value);
	}
}


/*
 * ForwardTuplesToClient forwards the tuples coming in over the connection
 * to the DestReceiver.
 */
static void
ForwardTuplesToClient(PGconn *connection, EState *executorState,
					  DestReceiver *destination)
{
	uint32 columnCount = 0;
	char **columnArray = NULL;
	TupleDesc tupleDescriptor = NULL;
	AttInMetadata *attributeInputMetadata = NULL;
	TupleTableSlot *tupleTableSlot = NULL;
	MemoryContext oldContext = NULL;
	PGresult *result = NULL;
	ExecStatusType resultStatus = 0;

	result = ReceiveUpstreamCommandResult(connection);
	resultStatus = PQresultStatus(result);
	if (resultStatus != PGRES_SINGLE_TUPLE && resultStatus != PGRES_TUPLES_OK)
	{
		PgMasqReportError(ERROR, result, connection, false);
	}

	/* initialise tuple descriptor from results */
	columnCount = PQnfields(result);
	tupleDescriptor = TupleDescriptorForResult(result); 
	attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);
	columnArray = (char **) palloc0(columnCount * sizeof(char *));

	/* start output */
	(*destination->rStartup)(destination, CMD_SELECT, tupleDescriptor);

	while (result != NULL)
	{
		uint32 rowIndex = 0;
		uint32 rowCount = 0;
		uint32 columnIndex = 0;

		resultStatus = PQresultStatus(result);
		if (resultStatus != PGRES_SINGLE_TUPLE && resultStatus != PGRES_TUPLES_OK)
		{
			PgMasqReportError(ERROR, result, connection, false);
		}

		rowCount = PQntuples(result);

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			HeapTuple heapTuple = NULL;
			memset(columnArray, 0, columnCount * sizeof(char *));

			for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				if (PQgetisnull(result, rowIndex, columnIndex))
				{
					columnArray[columnIndex] = NULL;
				}
				else
				{
					columnArray[columnIndex] = PQgetvalue(result, rowIndex, columnIndex);
				}
			}

			oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(executorState));
			heapTuple = BuildTupleFromCStrings(attributeInputMetadata, columnArray);
			MemoryContextSwitchTo(oldContext);

			ExecStoreTuple(heapTuple, tupleTableSlot, InvalidBuffer, true);

			(*destination->receiveSlot)(tupleTableSlot, destination);
			executorState->es_processed++;

			ExecClearTuple(tupleTableSlot);
			ResetPerTupleExprContext(executorState);
		}

		PQclear(result);

		result = ReceiveUpstreamCommandResult(connection);
	}

	MemoryContextSwitchTo(oldContext);

	/* shutdown the tuple receiver */
	(*destination->rShutdown)(destination);

	ExecDropSingleTupleTableSlot(tupleTableSlot);
}


/*
 * TupleDescriptorForResult builds a TupleDescriptor from a PGresult.
 */
static TupleDesc
TupleDescriptorForResult(PGresult *result)
{
	int columnCount = PQnfields(result);
	int columnIndex = 0;
	TupleDesc tupleDescriptor = CreateTemplateTupleDesc(columnCount, false);

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		TupleDescInitEntry(tupleDescriptor,
						   columnIndex + 1,
						   PQfname(result, columnIndex),
						   PQftype(result, columnIndex),
						   PQfmod(result, columnIndex),
						   0);
	}

	return tupleDescriptor;
}


/*
 * ForwardCommandResultsToClient forwards the result of a command (DML, DDL)
 * to the client by setting the executor state.
 */
static void
ForwardCommandResultToClient(PGconn *connection, EState *executorState,
							 char *completionTag)
{
	char *currentAffectedTupleString = NULL;
	PGresult *result = NULL;
	ExecStatusType resultStatus = 0;

	result = ReceiveUpstreamCommandResult(connection);
	resultStatus = PQresultStatus(result);
	if (resultStatus != PGRES_COMMAND_OK)
	{
		PgMasqReportError(ERROR, result, connection, false);
	}

	currentAffectedTupleString = PQcmdTuples(result);
	if (currentAffectedTupleString != NULL &&
		strlen(currentAffectedTupleString) > 0)
	{
		executorState->es_processed = pg_atoi(currentAffectedTupleString,
											  sizeof(int32), 0);
	}

	if (completionTag != NULL)
	{
			strncpy(completionTag, PQcmdStatus(result), COMPLETION_TAG_BUFSIZE);
	}

	PQclear(result);

	while ((result = ReceiveUpstreamCommandResult(connection)) != NULL)
	{
		PQclear(result);
	}
}


/*
 * ReceiveUpstreamCommandResult blocks waiting for the result of a command
 * that was sent upstream and simulataneously checks for cancellation.
 */
PGresult *
ReceiveUpstreamCommandResult(PGconn *connection)
{
	int socket = 0;
	int waitFlags = WL_POSTMASTER_DEATH | WL_LATCH_SET;
	bool wasNonblocking = false;
	PGresult *result = NULL;
	bool failed = false;

	/*
	 * Short circuit tests around the more expensive parts of this
	 * routine. This'd also trigger a return in the, unlikely, case of a
	 * failed/nonexistant connection.
	 */
	if (!PQisBusy(connection))
	{
		return PQgetResult(connection);
	}

	socket = PQsocket(connection);
	wasNonblocking = PQisnonblocking(connection);

	/* make sure not to block anywhere */
	if (!wasNonblocking)
	{
		PQsetnonblocking(connection, true);
	}

	CHECK_FOR_INTERRUPTS();

	/* make sure command has been sent out */
	while (!failed)
	{
		int rc = 0;

		ResetLatch(MyLatch);

		/* try to send all the data */
		rc = PQflush(connection);

		/* stop writing if all data has been sent, or there was none to send */
		if (rc == 0)
		{
			break;
		}

		/* if sending failed, there's nothing more we can do */
		if (rc == -1)
		{
			ResetConnection();

			ereport(ERROR, (errmsg("failed to send data upstream")));
		}

		/* this means we have to wait for data to go out */
		Assert(rc == 1);

		rc = WaitLatchOrSocket(MyLatch, waitFlags | WL_SOCKET_WRITEABLE, socket, 0);

		if (rc & WL_POSTMASTER_DEATH)
		{
			ResetConnection();

			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		if (rc & WL_LATCH_SET)
		{
			CHECK_FOR_INTERRUPTS();
		}
	}

	/* wait for the result of the command to come in */
	while (!failed)
	{
		int rc = 0;

		ResetLatch(MyLatch);

		/* if reading fails, there's not much we can do */
		if (PQconsumeInput(connection) == 0)
		{
			ResetConnection();

			ereport(ERROR, (errmsg("failed to receive data from upstream")));
		}

		/* check if all the necessary data is now available */
		if (!PQisBusy(connection))
		{
			result = PQgetResult(connection);
			break;
		}

		rc = WaitLatchOrSocket(MyLatch, waitFlags | WL_SOCKET_READABLE, socket, 0);

		if (rc & WL_POSTMASTER_DEATH)
		{
			ResetConnection();

			ereport(ERROR, (errmsg("postmaster was shut down, exiting")));
		}

		if (rc & WL_LATCH_SET)
		{
			CHECK_FOR_INTERRUPTS();

			/*
			 * If raising errors allowed, or called within in a section with
			 * interrupts held, return NULL instead, and mark the transaction
			 * as failed.
			 */
			if (InterruptHoldoffCount > 0 && (QueryCancelPending || ProcDiePending))
			{
				failed = true;
				break;
			}
		}
	}

	if (!wasNonblocking)
	{
		PQsetnonblocking(connection, false);
	}

	return result;
}


/*
 * PgMasqReportError is adapted from pgfdw_report_error in postgres_fdw.
 */
void
PgMasqReportError(int errorLevel, PGresult *result, PGconn *connection,
				  bool closeConnectionOnError)
{
	/* If requested, PGresult must be released before leaving this function. */
	PG_TRY();
	{
		char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
		char *primaryMessage = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
		char *detailMessage = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
		char *hintMessage = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
		char *contextMessage = PQresultErrorField(result, PG_DIAG_CONTEXT);
		int sqlState = 0;

		if (sqlStateString)
		{
			sqlState = MAKE_SQLSTATE(sqlStateString[0],
									 sqlStateString[1],
									 sqlStateString[2],
									 sqlStateString[3],
									 sqlStateString[4]);
		}
		else
		{
			sqlState = ERRCODE_CONNECTION_FAILURE;
		}

		/*
		 * If we don't get a message from the PGresult, try the PGconn.  This
		 * is needed because for connection-level failures, PQexec may just
		 * return NULL, not a PGresult at all.
		 */
		if (primaryMessage == NULL && connection != NULL)
		{
			primaryMessage = PQerrorMessage(connection);
		}

		ereport(errorLevel,
				(errcode(sqlState),
				 primaryMessage ? errmsg_internal("%s", primaryMessage) :
				 errmsg("could not obtain message string for remote error"),
				 detailMessage ? errdetail_internal("%s", detailMessage) : 0,
				 hintMessage ? errhint("%s", hintMessage) : 0,
				 contextMessage ? errcontext("%s", contextMessage) : 0));
	}
	PG_CATCH();
	{
		ClearResults(connection);

		if (closeConnectionOnError)
		{
			ResetConnection();
		}

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * ClearResults clears all results to get the connection ready for the
 * next commands.
 */
void
ClearResults(PGconn *connection)
{
	PGresult *result = NULL;

	while ((result = PQgetResult(connection)) != NULL)
	{
		PQclear(result);
	}

}


/*
 * FinishUpstreamTransaction finishes the transaction on the upstream node.
 * If isCommit is true we send COMMIT and throw an error in case of failure.
 * If isCommit is false we send ROLLBACK and show a warning in case of
 * failure. 
 */
void
FinishUpstreamTransaction(bool isCommit)
{
	PGconn *connection = UpstreamConnection;
	PGresult *result = NULL;
	char *command = NULL;
	int errorLevel = 0;

	if (connection == NULL)
	{
		return;
	}

	if (isCommit)
	{
		command = "COMMIT";
		errorLevel = ERROR;
	}
	else
	{
		command = "ROLLBACK";
		errorLevel = WARNING;
	}

	elog(DEBUG4, "pgmasq sending upstream: %s", command);

	SendCommandUpstream(connection, command, NULL);

	while ((result = ReceiveUpstreamCommandResult(connection)) != NULL)
	{
		ExecStatusType resultStatus = PQresultStatus(result);
		if (resultStatus != PGRES_COMMAND_OK)
		{
			PgMasqReportError(errorLevel, result, connection, true);
		}

		PQclear(result);
	}

}


/*
 * ResetConnection closes the currently active connection.
 */
void
ResetConnection(void)
{
	if (UpstreamConnection != NULL)
	{
		PQfinish(UpstreamConnection);
		UpstreamConnection = NULL;
	}
}


/*
 * DisallowReconnect ensures that no more reconnects are allowed.
 * This can be used to ensure we don't lose session state on one
 * side due to connection loss.
 */
void
DisallowReconnect(void)
{
	ConnectAllowed = false;
}
