#pragma once

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"


void pg_get_query_def(Query *query, StringInfo buffer);
