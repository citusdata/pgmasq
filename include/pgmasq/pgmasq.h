#pragma once

#define EXECUTE_LOCALLY_NONE 0
#define EXECUTE_LOCALLY_SELECTS 1
#define EXECUTE_LOCALLY_IMMUTABLE 2
#define EXECUTE_LOCALLY_ALL 3

#ifndef tuplecount_t
#if (PG_VERSION_NUM >= 90600)
#define tuplecount_t uint64
#else
#define tuplecount_t long
#endif
#endif
