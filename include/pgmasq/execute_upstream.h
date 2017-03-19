#pragma once


/* GUCs */
extern char *ConnectionInfoOverride;


/* public functions */
extern void PgMasqQuery(const char *queryString, ParamListInfo params, EState *estate,
						bool sendTuples, DestReceiver *dest, char *completionTag);
extern List * ExecuteSimpleQueryUpstream(const char *queryString);
extern PGconn * OpenUpstreamConnection(void);
extern void SendCommandUpstream(PGconn *connection, const char *command,
								ParamListInfo paramListInfo);
extern PGresult * ReceiveUpstreamCommandResult(PGconn *connection);
extern void PgMasqReportError(int errorLevel, PGresult *result, PGconn *connection,
							  bool closeConnectionOnError);
extern void ClearResults(PGconn *connection);
extern void FinishUpstreamTransaction(bool isCommit);
extern void ResetConnection(void);
extern void DisallowReconnect(void);
