# pgmasq

Pgmasq transparently forward transactions from a hot standby to a primary to enable DDL and DML from any node.

# Installation

Compile the extension from source:

```
git clone https://github.com/citusdata/pgmasq.git
cd pgmasq
make && sudo PATH=$PATH make install
```

# Setting up pgmasq

Add the following to postgresql.conf:

```
shared_preload_libraries = 'pgmasq'
pgmasq.execute_locally = immutable # one of: none, immutable, selects
lock_timeout = '2s'
```

The `pgmasq.execute_locally` setting controls which queries are executed locally on the standby. The following settings are supported:

- `none` forwards all commands to the primary.
- `immutable` executes selects outside of a transactino block that contain no functions that could modify the database on the standby.
- `selects` executes all selects outside of a transaction block on the standby.

Additionally, we recommend setting `synchronous_commit = remote_apply` on the primary to provide read-your-writes consistency.

To enable pgmasq in a particular database, run `CREATE EXTENSION pgmasq` in that database on the primary.

# Authentication

For pgmasq to work your replication user also needs to be superuser. You can make that the case by running:

```
ALTER USER replicator SUPERUSER;
```

In addition, you may need to add a line to pg_hba.conf to ensure access to the database, in addition to replication access:

```
host replication replicator 10.0.0.0/8 md5
host all replicator 10.0.0.0/8 md5
```

# Limitations

`CREATE TEMPORARY TABLE` is not supported as the parser on the hot standby cannot see the temporary table.

Multi-statement transactions of the form `BEGIN; [DDL on table X]; [SELECT/DML on table X]; COMMIT;` will create a *deadlock*, since the DDL will take an exclusive lock, which will prevent the parser from proceeding with the SELECT/DML.

