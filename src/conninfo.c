/*-------------------------------------------------------------------------
 *
 * src/conninfo.c
 *
 * This file contains functions to get the primary connection info from
 * recovery.conf.
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

#include "pgmasq/conninfo.h"
#include "storage/fd.h"
#include "utils/guc.h"


#define RECOVERY_COMMAND_FILE "recovery.conf"


/*
 * ReadPrimaryConnInfoFromRecoveryConf gets the unaltered primary_conninfo
 * field from the recovery.conf file.
 */
char *
ReadPrimaryConnInfoFromRecoveryConf(void)
{
	FILE *fd = NULL;
	ConfigVariable *item = NULL;
	ConfigVariable *head = NULL;
	ConfigVariable *tail = NULL;
	char *primaryConnInfo = NULL;

	fd = AllocateFile(RECOVERY_COMMAND_FILE, "r");
	if (fd == NULL)
	{
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open recovery command file \"%s\": %m",
						RECOVERY_COMMAND_FILE)));
	}

	/*
	 * Since we're asking ParseConfigFp() to report errors as FATAL, there's
	 * no need to check the return value.
	 */
	(void) ParseConfigFp(fd, RECOVERY_COMMAND_FILE, 0, FATAL, &head, &tail);

	FreeFile(fd);

	for (item = head; item; item = item->next)
	{
		if (strcmp(item->name, "primary_conninfo") == 0)
		{
			primaryConnInfo = pstrdup(item->value);
			ereport(DEBUG2,
					(errmsg_internal("primary_conninfo = '%s'",
									 primaryConnInfo)));
		}
	}

	FreeConfigVariables(head);

	return primaryConnInfo;
}
