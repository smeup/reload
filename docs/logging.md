# Logging

This document contains information about reload logging system usage.

## General information

Reload has a built in callback type logging system, logger class costructor takes a callback function as parameter. This function, supplied by reload users, is notified with log events in order to perform the log operation.

The use of a callback system allows to totally uncuple Reload from specific logging api dependencies, relegating to the caller the task of physically performing logging operations. 

## What reload logs
Every log event is identified by a logging key representing the scope within the log take place, every logging key is associated with a log level:
* `native_access_method` (TRACE)
* `read_data` (TRACE)
* `execute_inquiry` (DEBUG)
* `search_data` (DEBUG)
* `connection` (DEBUG)

Reload provide built in log events generation for most relevant aspects of every logging key.

## Logging enabling
Logger instance is passed as a parameter constructor to `DBNativeAccessConfig` class, this will then be used for all the operations performed with `ConnectionConfig` configured on it.

## Logger interfaces

Logger class:

`com.smeup.dbnative.log.Logger`

takes two constructor parameters:

* Callback function
* Log level

### Callback function

Callback function `(LoggingEvent) -> (Unit)` is provided by Reload user who has freedom of choice on log system and on event informations to be used. 
Reload, when calling the log function, generates a `LoggingEvent` that will be consumed by set logging function

The event consists of the following informations:

* Logging key
* Log message
* Reload class, method and row that raised the event
* Date and time of event generation
* Native method whose execution raised the event
* File used
* Event performance. If the event is measurable, ie associated with the execution of a block of code, the execution time in milliseconds


### Log level

Enumeration:

`com.smeup.dbnative.log`

defines usable logs level:

`OFF, ERROR, WARN, INFO, DEBUG, TRACE, ALL`

Each logging key is associated with a log level. By setting a level X callback function will be notified only for events with level <= ordinality (X)
