# Reload: a record level access library for modern databases 
![reload Logo](/images/logo-reload-small.png)  

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)



## A short introduction to Reload

**Reload** is a library written in Kotlin and compatible with Java 8 (and later version). The main purpose of the Reload library is provide simple api for record-level access to modern databases, as an alternative to classic access methods (SQL or native methods).

## What is Record level access

Reload offer a record level access to modern database. But, what is a record level access and how it works?

Sequential record level access is a method for accessing the data contained in a database created by **IBM** and widely used in programs written in RPG language that run on iSeries systems (formerly AS400) and access data on DB2 / 400 databases

The basic element is a *file*, intended as a table with columns and rows. The structure of the table is defined through *metadata* that define the nature of the columns (data types, size, etc.). Metadata define also key columns, intended as columns that will be keys in search function. 

Obviusly, datas are contained in this file rows.

With record level access you can explore the data contained in the file in a sequential manner, record by record.

A tipical search job with record level access is done with this steps:

1. Define a file (new or existing) recording its metadata in Reload enviroment. This is a one time operation, when a new file is registered its definition is persistent. Of course, you can always delete or modify an existing registration.

2. Open the file and lock it

3. Filter file contents with a set of values assigned to the keys. This operation creates a subset of datas that satisfies the search conditions and return a cursor object that point to the first element of the subset.

4. Use the cursor created in the previous step to read the data sequentially. You can read datas forward or backward and you can apply further  filters on previous filtered datas. Every read operation return a single record (file row) that you can manage.

5. Close file and unlock it.

All the steps described in previous points are made invoking specific api provided by Reload library.
  

## Reload is DBM agnostic

Reload is not RDBM specfic and grant sequential access to datas contained in several types of databases:

- SQLDBM, as MySQL, Oracle, MSSQL and more (all DBMs with an JDBC driver avalilable)

- NOSQL database, as MongoDB, OrientDB and more

- No standard persitent data engines

Reload has a modular architecture based on a common interface. You can use exiting modules for sequential access to well knowns DBM or write new module for sequential access to data stored in your own  persistance environment. 
 
## How to use this code in your project

We use [Maven Central](https://repo1.maven.org/maven2/.) to publish the [project](https://repo1.maven.org/maven2/io/github/smeup/reload/).
See more details [here](docs/central.md).

### Maven
If you use Maven, add the following dependencies for the core library:
	
    <dependency>
        <groupId>com.github.smeup.reload</groupId>
        <artifactId>reload</artifactId>
        <version>development-SNAPSHOT</version>
    </dependency>

### Gradle
Here are the configurationd to add to your build.gradle:
```
dependencies {
    ...
    implementation 'com.github.smeup:reload:-SNAPSHOT'
}
```

### Build a single jar file distribution

Thanks to assembly maven plugin it is possible to create a single jar file distribution
for library Reload.

In the maven root module, simply launch the command:

```
mvn clean package -DskipTests=true
```

If command ends successfully, you can find the single jar file under the directory **target** under 
the module **distribution**.

The produced file will have a name with a format like this:

```
reload-<version>-jar-with-dependencies.jar
```

Version of produced fatjar file is the version setted in maven modules; you can simpley change this version in your entire maven project
running this maven commands:

```
mvn versions:set -DnewVersion=<new_version_in maven_format>
mvn versions:commit
```

For example, to produce a new 1.2.0-SNAPSHOT version of Reload as fatjar you have
to do this steps:

```
mvn versions:set -DnewVersion=1.2.0-SNAPSHOT
mvn versions:commit
mvn clean package -DskipTests=true
```

and the result will be a file

```
reload-1.2.0-SNAPSHOT.jar-with-dependencies.jar
```

contained in the **distribution/target** folder.

## Example of using the Reload library

At [this page](docs/example.md) there is a simple example of utilization of the Reload library for access

## Enabling logs in Reload

See more details [in this document](docs/logging.md).

## Additional license info

- Reload project uses [JT400Open library](https://sourceforge.net/projects/jt400/) distribuited under IBM IPL 1.0 license. See [this link](https://opensource.org/licenses/ibmpl.php) for license detail.


