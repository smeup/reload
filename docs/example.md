# Example: access to a database table with Reload

In this simple example, we show how to access a DB table using Reload library.

The procedure is based on four distinct steps:

1. Define and register the structure of the table we want to access
2. Configure the access information to physical table 
3. Creation of a DBFile manager
4. Creation of DBFile instance that allows read, write and delete operations 
   on opened table   


## Step one: define and register the structure of the table we want to access

Before opening a table it is necessary to define its structure and register the information in Reload. The table structure
is defined using an instance of a Metadata class, that define the list of columns and the list of columns that are keys for the
desired tabel. Metadata don't contains information about the columns data type and lenght because
Reload manage all datas as Text and has not limits in their lengths.

**Note**: by design choice, the Reload library cannot create tables but only open and manage existing ones. Indeed, the information
contained in the metadata are not necessary for the normal functioning of the library. But this information may be needed for external products that
use the Reload library for accessing tables and may need information on their structure. Reload could read this information from
physical table, but by design choice we prefer to store the information about the structure in an external repository and limit dependencies
between the reload code and the connection to the physical table.

So, the first step is the creation of a Metadata object:

```sh
var fields = mutableListOf<TypedField>();
fields.add(Field("NAZ", "Nation"));
fields.add(Field("REG", "Region"));
fields.add(Field("PRO", "Province"));
fields.add(Field("CIT", "City"));
fields.add(Field("PRE", "Prefix"));
fields.add(Field("COD", "ISTAT code"));

var keys = mutableListOf<String>();
keys.add("NAZ");
keys.add("REG");
keys.add("PRO");
keys.add("CIT");

val municipalityTableMetadata = FileMetadata("MUNICIPALITY", null, fileds, keys)
```

This code define a MUNICIPALITY table, with 6 columns defined by name and 
description and 4 keys. All information are contained in a FileMetadata instance
named **municipalityTableMetadata**.

## Step two: configure the access information to physical table

Now we have to define where is the real table defined in the previous step as metadata.
Reload can access different types of physical tables, both sql and nosql.

Phisycal access to a real table is defined as a DBNativeAccessConfig instance.

```sh

var connectionConfigs = mutableListOf<ConnectionConfig>;

connectionConfigs.add(
    ConnectionConfig(
            fileName= "MUNICIPALITY",
            url = "jdbc:hsqldb:mem:TESTDB",
            user = "user",
            password = "password",
            driver = "org.hsqldb.jdbcDriver"
    )
);

connectionConfigs.add(
    ConnectionConfig(
        fileName= "*-MONGO",
        url = "mongodb://localhost:27017/TESTDB",
        user = "user",
        password = "password")
);  

var dbConfig = DBNativeAccessConfig(connectionConfigs);
```
In this example, we define two table connection: the first one says that the table
MUNICIPALITY (defined as metadata in step one) point to a phisycal table with the
same name hosted in a Hypersonic DB called TESTDB. The connection config contains all 
the information required for a phisical access to the SQL table (name, user, password,
driver).

In the example there is also a second ConnectionConfig definition that configure a 
phisical access to noSQL tables saved on a MongoDB server. This example shows how 
wildcards can be used to simultaneously define connections for multiple tables.

In fact, the notation *-MONGO means:

_All tables with name that ends with -MONGO are hosted on the MongoDB server 
identified by this ConnectionConfig definition_

After the connection config list is defined, we can create an instance of 
**DBNativeAccessConfig** passing the connection config list as parameter.

## Step three: creation of a DBFileManager.

In previous steps we have defined table structures and physical table connections. Now
we are ready to create a DBFileManger object that allow the access to pysical table..

```sh

DBFileFactory.registerMetadata(municipalityTableMetadata);
var dbFileManager = DBFileManager(dbConfig);

```
The first operation register in DBFileManager the metadata definition of MUNICIPALITY
table. Registration is persistent, so there is no need to repeat this registration
command unless the table structure is redefined.
Reload saves the metadata in properties files contained in a specific directory of 
the file system and considers a metadata as registered as long as the relative properties 
file exists. 
Note that these properties files can also be loaded directly into the registration 
directory avoiding the registration operation in code. For example, the properties file
relative to a specific table can be created and saved in the register directory by external
tools (for example, tools for database migrations).

The second operation create a **dbFileManager** as instance of class DBFileManager. With this
object we can open all tables defined in the dbConfig instance passed as parameter.

**Note:** for external tools that need information about the structure of a single table, the class
DBFileFactory offer static methods for reading metadata of a registered table, identified by its name.


## Step four: creation of DBFile instance that allows read, write and delete operations on opened table   

Now we are ready to open a table and operate on it.

```sh
var municipalityDBFile = dbFileManager.open("MUNICIPALITY", null);
```

This operation create a **municipalityDBFile** instance of DBFile class that 
allow full access to the MUNICIPALITY table. Now we have all what we need for searching,
reading, writing and deleting records on this table.

**PS:** note that open method has a second parameter, in this case setted to null. In this second
parameter we could pass a fileMetadata instance and in this case the open method make two operation:
registering passed metadata as metadata for file "MUNICIPALITY" and opening the file.

Note that DBFile open method create a connection with physical table hosted on backend 
DBM but at code level there is no difference beetwen SQL and noSQL connections. Access to
a table is always obtained with a call to the open method and all subsequent operations on 
the table will have the same syntax regardless of the type of database we are connected to.

A simple example of operations on table:

```sh

municipalityDBFile.setLL("ITA", "LOMBARDIA", "BG");

Result readRecord = municipalityDBFile.readEqual("ITA", "LOMBARDIA", "BG");
readRecord = municipalityDBFile.readEqual("ITA", "LOMBARDIA", "BG");
readRecord = municipalityDBFile.readEqual("ITA", "LOMBARDIA", "BG");
Result readRecord = municipalityDBFile.readPrevious("ITA", "LOMBARDIA", "BG");


municipalityDBFile.close();

```

In this simple case, the first statement point before the first record in the MUNICIPALITY that match the
three passed keys. After pointment, we read the next three records, saving data in an record object obtained as result from single method call
And after three read in forward direction, we decide to invert the direction end read the previous record.

This simple example shows that using methods offered by municipalityDBFile (as instance of DBFile interface) we can navigate in table 
content managing a cursor as a pointer to a specific record and navigate forward and also backward in sequential mode. 
We can always change the position of cursor in the table calling a positioning method (as chain, setLL or setGT).

When the navigation in the table is finished, is important to remember to close the connection, so
the system can release all the allocated resources.
