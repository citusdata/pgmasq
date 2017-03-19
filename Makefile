#-------------------------------------------------------------------------
#
# Makefile for pgmasq
#
# Copyright (c) 2017, Citus Data, Inc.
#
#-------------------------------------------------------------------------

EXTENSION = pgmasq
EXTVERSION = 1.0

# installation scripts
DATA = $(wildcard updates/*--*.sql)
DATA_built += $(EXTENSION)--$(EXTVERSION).sql

# documentation and executables
DOCS = $(wildcard doc/*.md)

# compilation configuration
MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))
PG_CPPFLAGS = -std=c99 -Wall -Wextra -Werror -Wno-sign-compare -Wno-unused-parameter -Iinclude -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)
EXTRA_CLEAN += $(addprefix src/,*.gcno *.gcda) # clean up after profiling runs

# test configuration
TESTS = $(sort $(wildcard test/sql/*.sql))
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-language=plpgsql
REGRESS_OPTS += --launcher=./test/launcher.sh # use custom launcher for tests

# add coverage flags if requested
ifeq ($(enable_coverage),yes)
PG_CPPFLAGS += --coverage
SHLIB_LINK += --coverage
endif

OS := $(shell uname)
ifeq ($(OS), Linux)
SHLIB_LINK += -Wl,-Bsymbolic
endif

# detect whether to build with pgxs or build in-tree
ifndef NO_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pgmasq
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# ensure MAJORVERSION is defined (missing in older versions)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

# if using a version older than PostgreSQL 9.3, abort
PG95 = $(shell echo $(MAJORVERSION) | grep -qE "8\.|9\.[012345]" && echo no || echo yes)
ifeq ($(PG95),no)
$(error PostgreSQL 9.6 or higher is required to compile this extension)
endif

# define build process for latest install file
$(EXTENSION)--$(EXTVERSION).sql: $(EXTENSION).sql
	cp $< $@
