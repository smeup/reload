# Reload: a record level access library for modern databases 
![reload Logo](/images/logo-reload-small.png)  

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![](https://jitpack.io/v/smeup/reload.svg)](https://jitpack.io/#smeup/reload)



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

At the moment, we use [Jitpack](https://jitpack.io/) to publish the [project](https://jitpack.io/#smeup/reload).
See more details [here](docs/jitpack.md).

### Maven
If you use Maven, add these lines to your pom.xml in order to add the repository

    <repositories>
        <repository>
            <id>jitpack.io</id>
            <url>https://jitpack.io</url>
        </repository>
    </repositories>
	
Then add the following dependencies for the core library:
	
    <dependency>
        <groupId>com.github.smeup</groupId>
        <artifactId>reload</artifactId>
        <version>development-SNAPSHOT</version>
    </dependency>

### Gradle
Here are the configurationd to add to your build.gradle:
```
allprojects {
    repositories {
        ...
        maven { url 'https://jitpack.io' }
    }
}
dependencies {
    ...
    implementation 'com.github.smeup:reload:-SNAPSHOT'
}
```

### Enabling logs

Reload logging can be enabled  with a simple property setting like this:

```
reload.logger.impl=<class implements logger interface>;<log level>;<logger scope>
```

Class that implements the logger interface in optional in this property definition; if not defined, Reload use a 
default implementation that write logs in console. If a class is passed in the property value, the implementation 
of this class must extend the following class defined in Reload:

```
com.smeup.dbnative.log.Logger
```
and the implementation must contain a costructor that accept as parameter an instance of this class:

```
com.smeup.dbnative.log.LoggingLevel
```

Log level is defined as one of the following default values:

- OFF 
- ERROR 
- WARN 
- INFO 
- DEBUG 
- TRACE 
- ALL


If a log level is not defined, ALL value will be used as default.

Finally, logger scope define the number of logger instances create by logger system. The available values
are two:

- CONFIG (default value if not passed): creates a single Logger instance for every instance of 
  DBNativeAccessConfig defined at startup.
- JVM: creates a single Logger instance for all DBNativeAccessConfig defined at startup.

Property reload.logger.impl could be defined in two alternatives ways:

* From system properties (best solution for Maven or Gradle projects)

```
    <profile>
        <id>reload-logging</id>
        <properties>
            <argLine>-Dreload.logger.impl=;DEBUG</argLine>
        </properties>
    </profile> 
```

* From properties file: best solution for develop environments, in this case Reload automatically search 
  and load from project resorces a file called `reload.properties` that contains the `reload.logger.impl` 
  value.

The system properties solution always takes priority over the properties file solution.

## Additional license info

- Reload project use [JT400Open library](https://sourceforge.net/projects/jt400/) distribuited under IBM IPL 1.0 license. See [this link](https://opensource.org/licenses/ibmpl.php) for license detail.


