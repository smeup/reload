# Development


This document contains information for developers who want to contribute to the the project.

## General information

Reload is packaged as a modular Maven project. If you want to modify and compile the source code and run the tests defined in the Junit modules some software must be installed on your development system:

1. *Maven*
2. *Docker*
3. *Docker-compose*

Docker and dockers are used for testing and allow you to create instances of DBMs and other necessary software quickly and easily. 
But in alternative, you can use software instances already installed on your system, if you have them.


## Get the code

Reload is a GitHub public project. Feel free to follow the project and download, analyze and modify the code.

Master branch on GitHub is protected and you can merge on this branch only after an approved pool request. Please work on personal branch or fork the entre project and then release upgrades as pool requests. 

Reload project is released with Apache 2.0 license, so if you create new files remember to insert the license header.  If you add an external library dependency to project, please control license rights before release software modifications.


## Compilation

To build the entire project go in the root and launch this command:

     mvn -Dmaven.test.skip=true package
     
This command compile all the code without tests execution.

## Compilation and test execution

To compile the entire project and run the tests, you must first activate the software needed to run the tests.

1. Go to *docker/mysql* directory and start a docker mysql instance with the command:

       docker-compose up -d
    
2. Go to *docker/rabbitmq* directory and start a docker rabbitmq instance with the command:

       docker-compose up -d

3. Compile entire project and execute tests with the command:

       mvn package    


## Relative Record Number (RRN) convention

RPG's Relative Record Number (`%RRN`, the INFDS RRN subfield, CHAIN/SETLL by RRN on unkeyed files) is
resolved by the SQL dialect (`SQLDialect.rrnSelectExpression`), and it is projected as `Result.rrn` on
every read, keyed or not:

| Database | RRN expression | Requirement |
|---|---|---|
| DB2 for i (`jdbc:as400`) | `RRN(<table>)` | none, it is native |
| everything else (PostgreSQL, HSQLDB, H2, MySQL, ...) | `<table>."__RNN"` | the table declares a `__RNN` column |

`__RNN` must be an auto-generated `BIGINT` primary key, assigned once at insert time (a stable, monotonic
value: RRN follows insertion order). The file's own declared keys become a `UNIQUE` constraint instead of
the primary key.

Reload does not compute `__RNN`, but it does probe for it once, at file open (`SQLDBFile`, via
`SQLDialect.requiresRrnColumn`/`Connection.hasColumn` - skipped entirely for DB2 for i, which needs no
column). If it's missing:
- The opportunistic output projection is simply skipped, on every read, keyed or not - `Result.rrn`
  stays `null`, same as on a backend with no RRN concept at all (JT400, NoSQL). No query fails because
  of it.
- An unkeyed file is addressed by RRN *itself* (CHAIN/SETLL/SETGT/READE/READPE by RRN) - there's no
  addressing mechanism without the column, so that specific operation still fails, but fast and with a
  clear message, before any SQL runs, rather than a raw driver "column ... does not exist".

```sql
-- PostgreSQL, HSQLDB, H2
CREATE TABLE "T" ("__RNN" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY, ..., UNIQUE("KEY"));
-- MySQL
CREATE TABLE "T" ("__RNN" BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, ..., UNIQUE("KEY"));
```

`START WITH 1` matters on HSQLDB, whose identity starts at 0 (PostgreSQL and H2 start at 1). Reload's own
test tables get the column from `SQLDBTestUtils.createFile` / `toSQL(url)`.

## Session tables (`__SID` / `__TSTAMP`)

A *session table* is a persistent table, shared by all the sessions, whose rows belong to one session each: it
replaces a temporary table (QTEMP-like) whose lifetime would otherwise be tied to a physical connection. A table
is a session table when it has the `__SID` column; reload then restricts every operation to the rows of the
session bound to the current scope, and the RPG side never sees the column (like `__RNN`, `__SID` and `__TSTAMP`
are **not** part of `FileMetadata.fields`, and are never keys).

The session id travels in the scope, not in the call stack:

```kotlin
ConnectionProvider.withScope("myApp", ReloadContext(sessionId = "..."), block)
ConnectionProvider.currentSessionIdOrNull()   // inside the scope
```

`ReloadContext` is the place for any further information bound to a scope, so `withScope` does not have to
change again.

| Operation | What reload does on a session table |
|---|---|
| CHAIN, SETLL/SETGT + READ/READP/READE/READPE, unpositioned READ, `%EQUAL`, page resume | adds `"__SID" = ?` (value bound first) to the `WHERE` of every statement, including the RRN ones |
| WRITE | adds `"__SID"` to the `INSERT` with the session id, whatever the caller sent. `__TSTAMP` is left to the database default |
| UPDATE, DELETE | the positioned ones go through the (already filtered) read cursor; the fallbacks for a read-only cursor (`updateByRrn`/`deleteByRrn`) add `AND "__SID" = ?` |

Whether a table is a session table (it has `__SID`) is decided lazily, at the **first operation** on the file, not
when it is opened, and cached per (database, user, table) like the `__RNN` probe. It has to be lazy: an application
may open a file before the table exists and create it afterwards (jariko opens the files when the program starts, a
work file may be created later in the same call). The answer "no `__SID`" is cached only when the table exists, so a
table created later is still recognised. It fails closed: an operation on a session table with no session bound to
the scope is an error, and so is a failing probe while a session is active, because carrying on would read, or
write, the rows of every session. Tables without `__SID` are not affected.

```sql
-- PostgreSQL, HSQLDB (Db2 for i: TIMESTAMP DEFAULT CURRENT TIMESTAMP)
CREATE TABLE "T" (
  "__RNN"    BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY,
  "__SID"    VARCHAR(64) NOT NULL,
  "__TSTAMP" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  ...);
CREATE INDEX "T_SID" ON "T" ("__SID");
```

- `__SID` is a `VARCHAR`, not a native `uuid`: every parameter is bound as a String, and a session id is an opaque
  string (a UUID today, 36 characters).
- **The index on `__SID` is required**: every statement carries `"__SID" = ?`, without it each read scans the
  rows of all the sessions. Put `__SID` first in any other index meant for the keys of the file.
- `__TSTAMP` is not used to filter: it is there to clean up the rows of sessions that never ended.
- A key of the file is unique only together with `__SID` (`UNIQUE("__SID", "KEY")`).

Known limitations:
- The table is found by its bare name: its schema (or library) must be on the connection's path, first, so it
  shadows a business table with the same name (`currentSchema` / `libraries` in the connection string). The
  `__SID` probe looks at the table name in any schema.
- `__RNN` is one identity for the whole table, so the RRNs of a session are not consecutive from 1: another
  session's rows take the numbers in between. Reading the RRN of a record and positioning on it later (CHAIN or
  SETLL by RRN) works, and never reaches another session's row; relying on the absolute value (e.g. "RRN 1 is the
  first record") does not.
