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

`__RNN` is an **unconditional contract**: reload does not compute it or check for it, so querying a table
that lacks it fails with "column ... does not exist". It must be an auto-generated `BIGINT` primary key,
assigned once at insert time (a stable, monotonic value: RRN follows insertion order). The file's own
declared keys become a `UNIQUE` constraint instead of the primary key.

```sql
-- PostgreSQL, HSQLDB, H2
CREATE TABLE "T" ("__RNN" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1) PRIMARY KEY, ..., UNIQUE("KEY"));
-- MySQL
CREATE TABLE "T" ("__RNN" BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, ..., UNIQUE("KEY"));
```

`START WITH 1` matters on HSQLDB, whose identity starts at 0 (PostgreSQL and H2 start at 1). Reload's own
test tables get the column from `SQLDBTestUtils.createFile` / `toSQL(url)`.
