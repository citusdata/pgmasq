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

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/explain.h"
#include "executor/execdesc.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "pgmasq/copy.h"
#include "pgmasq/execute_upstream.h"
#include "pgmasq/pgmasq.h"
#include "pgmasq/ruleutils.h"
#include "portability/instr_time.h"
#include "replication/walreceiver.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/predicate.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/guc_tables.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"


extern TransactionId XactTopTransactionId;

/* controls whether pgmasq should forward queries */
static int ExecuteLocally = EXECUTE_LOCALLY_ALL;

/* where the current query is being executed */
static bool ForceLocalExecution = false;
static bool UpstreamTransactionBlockOpen = false;
static bool SessionUserIsSuperuser = false;

/* saved hook values in case of unload */
static planner_hook_type PreviousPlannerHook = NULL;
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;
static ExecutorRun_hook_type PreviousExecutorRunHook = NULL;
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static ExplainOneQuery_hook_type PreviousExplainOneQueryHook = NULL;


/* private function declarations */
extern void _PG_init(void);
static bool IsPgMasqActive(void);
static PlannedStmt * PgMasqPlanner(Query *parse, int cursorOptions,
								   ParamListInfo boundParams);
static bool  contain_sequence_expression_walker(Node *node, void *context) ;
static void PlanUpstreamQueryExecution(PlannedStmt *result, Query *parse);
static Node * PgMasqCreateScan(CustomScan *scan);
static void PgMasqBeginScan(CustomScanState *node, EState *estate, int eflags);
static void PgMasqEndScan(CustomScanState *node);
static void PgMasqExplain(Query *query, IntoClause *into, ExplainState *es, 
						  const char *queryString, ParamListInfo params);
static void PgMasqExecutorStart(QueryDesc *queryDesc, int eflags);
static void PgMasqExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
							  tuplecount_t count);
static char * GetContainedQueryString(PlannedStmt *result);
static void PgMasqProcessUtility(Node *parsetree, const char *queryString,
								 ProcessUtilityContext context, ParamListInfo params,
								 DestReceiver *dest, char *completionTag);
static void PgMasqXactHandler(XactEvent event, void *arg);
static void OverrideSetSessionAuthorization(void);
static bool CheckSessionAuthorization(char **newval, void **extra,
											  GucSource source);
static void AssignSessionAuthorization(const char *newval, void *extra);


/* GUC options */
static const struct config_enum_entry execute_mode_options[] = {
	{ "all", EXECUTE_LOCALLY_ALL, false },
	{ "selects", EXECUTE_LOCALLY_SELECTS, false },
	{ "immutable", EXECUTE_LOCALLY_IMMUTABLE, false },
	{ "none", EXECUTE_LOCALLY_NONE, false },
	{ NULL, 0, false }
};

/* CustomScan functions */
static CustomScanMethods PgMasqCustomScanMethods = {
	"PgMasq",
	PgMasqCreateScan
};

static CustomExecMethods PgMasqCustomExecMethods = {
	.CustomName = "PgMasqScan",
	.BeginCustomScan = PgMasqBeginScan,
	.ExecCustomScan = NULL,
	.EndCustomScan = PgMasqEndScan,
	.ReScanCustomScan = NULL,
	.ExplainCustomScan = NULL
};


/* declarations for dynamic loading */
PG_MODULE_MAGIC;

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(pgmasq_reauthenticate);


/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("pqmasq can only be loaded via shared_preload_libraries"),
						errhint("Add pqmasq to shared_preload_libraries configuration "
								"variable in postgresql.conf.")));
	}

	PreviousPlannerHook = planner_hook;
	planner_hook = PgMasqPlanner;

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = PgMasqExecutorStart;

	PreviousExecutorRunHook = ExecutorRun_hook;
	ExecutorRun_hook = PgMasqExecutorRun;

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = PgMasqProcessUtility;

	PreviousExplainOneQueryHook = ExplainOneQuery_hook;
	ExplainOneQuery_hook = PgMasqExplain;

	DefineCustomEnumVariable(
		"pgmasq.execute_locally",
		gettext_noop("Sets which commands to forward while in recovery mode"),
		NULL,
		&ExecuteLocally,
		EXECUTE_LOCALLY_ALL,
		execute_mode_options,
		PGC_POSTMASTER,
		0,
		NULL,
		NULL,
		NULL);

	DefineCustomStringVariable(
		"pgmasq.conninfo",
		gettext_noop("Connection info of the node to which to forward commands"),
		NULL,
		&ConnectionInfoOverride,
		NULL,
		PGC_POSTMASTER,
		0,
		NULL,
		NULL,
		NULL);

	RegisterXactCallback(PgMasqXactHandler, NULL);
}


/*
 * PgMasqPlanner overrides the planner to intercept queries that need to be executed
 * upstream.
 */
static PlannedStmt *
PgMasqPlanner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *plannedStatement = NULL;
	bool executeUpstream = false;
	Query *parseCopy = NULL;
	bool isPgMasqActive = IsPgMasqActive();

	if (!isPgMasqActive && UpstreamTransactionBlockOpen)
	{
		ereport(ERROR, (errmsg("cannot finish upstream transaction")));
	}

	if (isPgMasqActive && !ForceLocalExecution)
	{
		if (ExecuteLocally == EXECUTE_LOCALLY_NONE || IsTransactionBlock())
		{
			/* execute all queries upstream */
			executeUpstream = true;
		}
		else if (parse->hasModifyingCTE || parse->commandType != CMD_SELECT ||
				 contain_sequence_expression_walker((Node *) parse, NULL))
		{
			/* query contains writes */
			executeUpstream = true;
		}
		else if (ExecuteLocally == EXECUTE_LOCALLY_IMMUTABLE &&
				 contain_volatile_functions((Node *) parse))
		{
			/* query contains one or more volatile function calls */
			executeUpstream = true;
		}
		else
		{
			/* query can be executed locally */
			executeUpstream = false;
		}

		if (executeUpstream)
		{
			/* get a copy of the query before the planner scribbles onit */
			parseCopy = copyObject(parse);
		}
	}

	if (PreviousPlannerHook != NULL)
	{
		plannedStatement = PreviousPlannerHook(parse, cursorOptions, boundParams);
	}
	else
	{
		plannedStatement = standard_planner(parse, cursorOptions, boundParams);
	}

	if (executeUpstream)
	{
		PlanUpstreamQueryExecution(plannedStatement, parseCopy);
	}

	return plannedStatement;
}


/*
 * IsPgMasqActive returns whether pgmasq should intercept queries.
 */
static bool
IsPgMasqActive(void)
{
	bool missingOK = true;
	Oid extensionOid = InvalidOid;

	if (ExecuteLocally == EXECUTE_LOCALLY_ALL)
	{
		return false;
	}

	if (!IsTransactionState())
	{
		return false;
	}

	if (!RecoveryInProgress())
	{
		return false;
	}

	extensionOid = get_extension_oid("pgmasq", missingOK);
	if (extensionOid == InvalidOid)
	{
		return false;
	}

	return true;
}


/*
 * contain_sequence_expression_walker walks over expression tree and returns
 * true if it contains calls to nextval or setval.
 */
static bool
contain_sequence_expression_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, contain_sequence_expression_walker,
								 context, 0);
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr *funcExpr = (FuncExpr *) node;

		if (funcExpr->funcid == F_NEXTVAL_OID ||
			funcExpr->funcid == F_SETVAL_OID ||
			funcExpr->funcid == F_SETVAL3_OID)
		{
			return true;
		}
	}

	return expression_tree_walker(node, contain_sequence_expression_walker, context);
}


/*
 * PlanUpstreamQueryExecution sets the query to forward upstream in the given plan.
 */
static void
PlanUpstreamQueryExecution(PlannedStmt *plan, Query *parse)
{
	CustomScan *customScan = makeNode(CustomScan);
	Const *queryData = NULL;
	StringInfo deparsedQuery = makeStringInfo();

	/* pass query serialized as a constant function argument */
	pg_get_query_def(parse, deparsedQuery);

	queryData = makeNode(Const);
	queryData->consttype = CSTRINGOID;
	queryData->constlen = deparsedQuery->len+1;
	queryData->constvalue = CStringGetDatum(deparsedQuery->data);
	queryData->constbyval = false;
	queryData->location = -1;

	customScan->scan.plan.targetlist = copyObject(plan->planTree->targetlist);
	customScan->custom_private = list_make1(queryData);
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
	customScan->methods = &PgMasqCustomScanMethods;

	plan->planTree = (Plan *) customScan;
	elog(DEBUG4, "pgmasq planner: %s", deparsedQuery->data);
}


/*
 *
 */
static Node *
PgMasqCreateScan(CustomScan *scan)
{
	CustomScanState *scanState = makeNode(CustomScanState);

	scanState->ss.ps.type = T_CustomScanState;
	scanState->methods = &PgMasqCustomExecMethods;

	return (Node *) scanState;
}


/*
 *
 */
static void
PgMasqBeginScan(CustomScanState *node, EState *estate, int eflags)
{
}


/*
 *
 */
static void
PgMasqEndScan(CustomScanState *node)
{
}


static void
PgMasqExplain(Query *query, IntoClause *into, ExplainState *es,
			  const char *queryString, ParamListInfo params)
{
	PlannedStmt *plan = NULL;
	instr_time  planstart;
	instr_time planduration;
	char *containedQueryString = NULL;

	INSTR_TIME_SET_CURRENT(planstart);

	/* plan the query */
	plan = pg_plan_query(query, into ? 0 : CURSOR_OPT_PARALLEL_OK, params);

	INSTR_TIME_SET_CURRENT(planduration);
	INSTR_TIME_SUBTRACT(planduration, planstart);

	containedQueryString = GetContainedQueryString(plan);
	if (containedQueryString != NULL)
	{
		List *explainLines = NIL;
		ListCell *explainLineCell = NULL;

		elog(DEBUG4, "pgmasq sending upstream: %s", queryString);

		explainLines = ExecuteSimpleQueryUpstream(queryString);

		foreach(explainLineCell, explainLines)
		{
			char *line = (char *) lfirst(explainLineCell);

			appendStringInfo(es->str, "%s\n", line);
		}
	}
	else if (PreviousExplainOneQueryHook != NULL)
	{
		PreviousExplainOneQueryHook(query, into, es, queryString, params);
	}
	else
	{
		ExplainOnePlan(plan, into, es, queryString, params, &planduration);
	}
}


/*
 * PgMasqExecutorRun actually runs a distributed plan, if any.
 */
static void
PgMasqExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *plannedStatement = queryDesc->plannedstmt;
	char *containedQueryString = NULL;

	containedQueryString = GetContainedQueryString(plannedStatement);
	if (containedQueryString != NULL)
	{
		EState *executorState = NULL;
		List *rangeTable = plannedStatement->rtable;
		Plan *plan = plannedStatement->planTree;

		eflags |= EXEC_FLAG_SKIP_TRIGGERS;

		/* build empty executor state to obtain memory contexts */
		executorState = CreateExecutorState();
		executorState->es_top_eflags = eflags;
		executorState->es_instrument = queryDesc->instrument_options;
		executorState->es_processed = 0;
		executorState->es_lastoid = InvalidOid;

		queryDesc->estate = executorState;

		/* check permissions early to avoid bugging the primary */
		ExecCheckRTPerms(rangeTable, true);

		queryDesc->tupDesc = ExecCleanTypeFromTL(plan->targetlist, false);
	}
	else if (PreviousExecutorStartHook != NULL)
	{
		PreviousExecutorStartHook(queryDesc, eflags);
	}
	else
	{
		standard_ExecutorStart(queryDesc, eflags);
	}
}


/*
 * PgMasqExecutorRun actually runs a distributed plan, if any.
 */
static void
PgMasqExecutorRun(QueryDesc *queryDesc, ScanDirection direction, tuplecount_t count)
{
	PlannedStmt *plannedStatement = queryDesc->plannedstmt;
	char *containedQueryString = NULL;

	containedQueryString = GetContainedQueryString(plannedStatement);
	if (containedQueryString != NULL)
	{
		EState *estate = queryDesc->estate;
		CmdType operation = queryDesc->operation;
		ParamListInfo paramListInfo = queryDesc->params;
		DestReceiver *destination = queryDesc->dest;
		MemoryContext oldcontext = NULL;
		bool sendTuples = false;

		if (!IsPgMasqActive())
		{
			ereport(ERROR, (errmsg("got an upstream query plan while pgmasq is inactive")));
		}

		if (ForceLocalExecution)
		{
			ereport(ERROR, (errmsg("got an upstream query plan while locally executing")));
		}

		if (queryDesc->totaltime != NULL)
		{
			InstrStartNode(queryDesc->totaltime);
		}

		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		sendTuples = (operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning);

		if (!UpstreamTransactionBlockOpen && IsTransactionBlock())
		{
			/* send ABORT in case of failure */
			UpstreamTransactionBlockOpen = true;
		}

		elog(DEBUG4, "pgmasq sending upstream: %s", containedQueryString);

		PgMasqQuery(containedQueryString, paramListInfo, estate, sendTuples,
					destination, NULL);

		if (queryDesc->totaltime != NULL)
		{
			InstrStopNode(queryDesc->totaltime, estate->es_processed);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		if (UpstreamTransactionBlockOpen)
		{
			ereport(ERROR, (errmsg("got a local query plan in an upstream transaction block")));
		}

		/*
		 * We're executing a query locally. Set the ForceLocalExecution flag to
		 * ensure that all other queries in the same transaction (mainly PL/pgSQL)
		 * are also executed locally, since we cannot suddenly switch to upstream
		 * execution inside a function.
		 */
		ForceLocalExecution = true;

		if (PreviousExecutorRunHook != NULL)
		{
			PreviousExecutorRunHook(queryDesc, direction, count);
		}
		else
		{
			standard_ExecutorRun(queryDesc, direction, count);
		}
	}
}


/*
 * GetContainedQueryString returns either NULL, if the plan does not contain
 * a masqed query, or the deparsed query string.
 */
static char *
GetContainedQueryString(PlannedStmt *result)
{
	CustomScan *customScan = NULL;
	Const *queryData = NULL;

	if (!IsA(result->planTree, CustomScan))
	{
		return NULL;
	}

	customScan = (CustomScan *) result->planTree;
	if (customScan->methods != &PgMasqCustomScanMethods)
	{
		return NULL;
	}

	queryData = (Const *) linitial(customScan->custom_private);

	return DatumGetCString(queryData->constvalue);
}


/*
 * PgMasqProcessUtility intercepts utility command to execute them
 * remotely on the primary, locally on the standby, or both.
 */
static void
PgMasqProcessUtility(Node *parseTree, const char *queryString,
					 ProcessUtilityContext context, ParamListInfo params,
					 DestReceiver *dest, char *completionTag)
{
	bool executeLocally = false;
	bool executeUpstream = false;
	bool copyTransactionId = false;
	bool sendTuples = false;
	bool disallowReconnect = false;
	bool isPgMasqActive = IsPgMasqActive();

	if (!isPgMasqActive ||
		ForceLocalExecution ||
		IsA(parseTree, VariableShowStmt) ||
		IsA(parseTree, ExecuteStmt))
	{
		/*
		 * Execute on standby if pgmasq is inactive or we're already in a local
		 * execution.
		 *
		 * pgmasq always executes SHOW, PREPARE and EXECUTE locally on the standby.
		 */
		executeLocally = true;
	}
	else if (IsA(parseTree, VariableSetStmt))
	{
		VariableSetStmt *setStatement = (VariableSetStmt *) parseTree;

		/*
		 * pgmasq executes SET commands both locally on the standby and on the
		 * primary to ensure a consistent state.
		 */
		executeLocally = true;
		executeUpstream = true;

		/*
		 * If we lose connection after a SET, then we lose all the prior SET
		 * commands and give unexpected results. We therefore disallow
		 * reconnects after a SET.
		 */
		if (!setStatement->is_local)
		{
			disallowReconnect = true;
		}
	}
	else if (IsA(parseTree, TransactionStmt))
	{
		TransactionStmt *transactionStatement = (TransactionStmt *) parseTree;

		/*
		 * pgmasq executes transaction commands (BEGIN, COMMIT, ROLLBACK) both
		 * locally on the standby and on the primary to ensure transaction state
		 * is set consistently.
		 */
		executeLocally = true;

		/*
		 * We'll send ROLLBACK from the commit handler so skip sending it here.
		 */
		if (transactionStatement->kind != TRANS_STMT_ROLLBACK &&
			transactionStatement->kind != TRANS_STMT_COMMIT)
		{
			executeUpstream = true;
		}

		if (transactionStatement->kind == TRANS_STMT_BEGIN)
		{
			copyTransactionId = true;
		}
	}
	else if (IsA(parseTree, PrepareStmt))
	{
		executeLocally = true;
		executeUpstream = true;
		disallowReconnect = true;
	}
	else if (IsA(parseTree, CopyStmt))
	{
		CopyStmt *copyStatement = (CopyStmt *) parseTree;

		if (copyStatement->is_from)
		{
			PgMasqCopyFrom(copyStatement, completionTag);
			return;
		}
		else
		{
			executeLocally = true;
		}
	}
	else if (IsA(parseTree, ExplainStmt))
	{
		executeUpstream = true;
		sendTuples = true;
	}
	else if (IsA(parseTree, CreateStmt))
	{
		CreateStmt *createStmt = (CreateStmt *) parseTree;

		if (createStmt->relation->relpersistence == RELPERSISTENCE_TEMP)
		{
			ereport(ERROR, (errmsg("cannot create temporary table via pgmasq")));
		}

		executeUpstream = true;
	}
	else
	{
		/*
		 * All other DDL is executed on the primary.
		 */
		executeUpstream = true;
	}

	if (executeLocally)
	{
		if (PreviousProcessUtilityHook != NULL)
		{
			PreviousProcessUtilityHook(parseTree, queryString, context,
									   params, dest, completionTag);
		}
		else
		{
			standard_ProcessUtility(parseTree, queryString, context,
									params, dest, completionTag);
		}
	}

	if (executeUpstream)
	{
		EState *estate = CreateExecutorState();

		if (!UpstreamTransactionBlockOpen && IsTransactionBlock())
		{
			UpstreamTransactionBlockOpen = true;
		}

		elog(DEBUG4, "pgmasq sending upstream: %s", queryString);

		PgMasqQuery(queryString, params, estate, sendTuples, dest, completionTag);
	}

	if (copyTransactionId)
	{
		const char *txidCurrentQuery = "SELECT txid_current()";
		List *transactionIdLines = NIL;
		char *txidCurrentString = NULL;
		TransactionId remoteTransactionId = InvalidTransactionId;

		transactionIdLines = ExecuteSimpleQueryUpstream(txidCurrentQuery);
		txidCurrentString = (char *) linitial(transactionIdLines);
		remoteTransactionId = pg_atoi(txidCurrentString, sizeof(uint32), 0);

		elog(DEBUG4, "pgmasq remote transaction: %d", remoteTransactionId);
	}

	if (disallowReconnect)
	{
		/*
		 * We've changed the state of the remote session in such a
		 * way that we cannot allow reconnects, as we would lose the
		 * state.
		 */
		DisallowReconnect();
	}
}


/*
 * PgMasqXactHandler resets execution flags at the end of a transaction.
 */
static void
PgMasqXactHandler(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		{
			if (UpstreamTransactionBlockOpen)
			{
				FinishUpstreamTransaction(false);
			}

			break;
		}

		case XACT_EVENT_PRE_COMMIT:
		{
			/*
			 * We commit upstream in pre-commit, since we may have to
			 * abort if the remote commit fails.
			 */ 
			if (UpstreamTransactionBlockOpen)
			{
				/*
				 * We release locks early to avoid deadlocking against the
				 * replication stream.
				 */
				ProcReleaseLocks(true);
				ReleasePredicateLocks(true);

				FinishUpstreamTransaction(true);
			}

			break;
		}

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
		{
			/* continue below */
			break;
		}

		default:
		{
			/* not end of transaction */
			return;
		}
	}

	/* reset flags at the end of the transaction */
	ForceLocalExecution = false;
	UpstreamTransactionBlockOpen = false;
}


/*
 * pgmasq_reauthenticate is a UDF that reauthenticates the session as the given user.
 * You must be superuser to call this function, since it gives you the ability to
 * connect as any user.
 */
Datum
pgmasq_reauthenticate(PG_FUNCTION_ARGS)
{
	text *roleNameText = PG_GETARG_TEXT_P(0);
	char *roleName = text_to_cstring(roleNameText);
	HeapTuple roleTuple = NULL;
	Oid roleId = InvalidOid;
	bool isSuperuser = false;

	if (!superuser())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				errmsg("must be superuser to reauthenticate")));
	}

	/* Look up the username */
	roleTuple = SearchSysCache1(AUTHNAME, PointerGetDatum(roleName));
	if (!HeapTupleIsValid(roleTuple))
	{
		GUC_check_errmsg("role \"%s\" does not exist", roleName);
		return false;
	}

	roleId = HeapTupleGetOid(roleTuple);
	isSuperuser = ((Form_pg_authid) GETSTRUCT(roleTuple))->rolsuper;

	ReleaseSysCache(roleTuple);

	/* set the session authorization as normal */
	SetSessionAuthorization(roleId, isSuperuser);

	/* now restrict changes to session authorization */
	SessionUserIsSuperuser = isSuperuser;

	OverrideSetSessionAuthorization();

	PG_RETURN_VOID();
}


/*
 * OverrideSetSessionAuthorization overrides the check and assign hooks that
 * are called when the user run SET SESSION AUTHORIZATION.
 */
static void
OverrideSetSessionAuthorization(void)
{
	struct config_generic **gucs = get_guc_variables();
	int numOpts = GetNumConfigOptions();
	int i = 0;

	for (i = 0; i < numOpts; i++)
	{
		struct config_generic *variable = (struct config_generic *) gucs[i];

		if (strcmp(variable->name, "session_authorization") == 0)
		{
			struct config_string *stringVariable = (struct config_string *) variable;

			stringVariable->check_hook = CheckSessionAuthorization;
			stringVariable->assign_hook = AssignSessionAuthorization;
		}
	}
}


/*
 * SET SESSION AUTHORIZATION
 */
typedef struct
{
	/* This is the "extra" state for both SESSION AUTHORIZATION and ROLE */
	Oid			roleid;
	bool		is_superuser;
} role_auth_extra;


/*
 * CheckSessionAuthorization is identical to check_role in
 * variable.c. We redefine it to not rely on a private struct in
 * AssignSessionAuthorization.
 */
static bool
CheckSessionAuthorization(char **newval, void **extra, GucSource source)
{
	HeapTuple	roleTup;
	Oid			roleid;
	bool		is_superuser;
	role_auth_extra *myextra;

	/* Do nothing for the boot_val default of NULL */
	if (*newval == NULL)
		return true;

	if (!IsTransactionState())
	{
		/*
		 * Can't do catalog lookups, so fail.  The result of this is that
		 * session_authorization cannot be set in postgresql.conf, which seems
		 * like a good thing anyway, so we don't work hard to avoid it.
		 */
		return false;
	}

	/* Look up the username */
	roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(*newval));
	if (!HeapTupleIsValid(roleTup))
	{
		GUC_check_errmsg("role \"%s\" does not exist", *newval);
		return false;
	}

	roleid = HeapTupleGetOid(roleTup);
	is_superuser = ((Form_pg_authid) GETSTRUCT(roleTup))->rolsuper;

	ReleaseSysCache(roleTup);

	/* Set up "extra" struct for assign_session_authorization to use */
	myextra = (role_auth_extra *) malloc(sizeof(role_auth_extra));
	if (!myextra)
		return false;
	myextra->roleid = roleid;
	myextra->is_superuser = is_superuser;
	*extra = (void *) myextra;

	return true;
}


/*
 * AssignSessionAuthorization ensures that we don't change the
 * session authorization after a pqmasq.reauthenticate(..) unless
 * the user is allowed to do so.
 */
static void
AssignSessionAuthorization(const char *newval, void *extra)
{
	role_auth_extra *myextra = (role_auth_extra *) extra;

	/* Do nothing for the boot_val default of NULL */
	if (!myextra)
	{
		return;
	}

	/* honour the session user */
	if (!SessionUserIsSuperuser)
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied to set session authorization")));
	}

	SetSessionAuthorization(myextra->roleid, myextra->is_superuser);
}
