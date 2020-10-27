# Logminer Kafka Connect

Logminer Kafka Connect is a CDC Kafka Connect source for Oracle Databases (tested with Oracle 11.2.0.4). 

Changes are extracted from the Archivelog using [Oracle Logminer](https://docs.oracle.com/cd/B19306_01/server.102/b14215/logminer.htm). 

- [Logminer Kafka Connect](#logminer-kafka-connect)
  * [Features](#features)
  * [Installation](#installation)
    + [Docker](#docker)
    + [Oracle Database Configuration Requirements](#oracle-database-configuration-requirements)
  * [Initial Import](#initial-import)
  * [Change Types](#change-types)
    + [Value Struct](#value-struct)
    + [Source struct](#source-struct)
  * [Configuration](#configuration)
  * [Limitations](#limitations)

## Features

Stable features:
- Insert, Update and Delete changes will be tracked on configured tables
- Logminer without "CONTINUOUS_MINE", thus in theory being compatible with Oracle 19c (not tested)
- Initial import of the current table state
- Only based on Oracle features that are available in Oracle XE (and thus available in all Oracle versions). No
Oracle GoldenGate license required!
- Reading schema changes from the Online Dictionary. See 
https://docs.oracle.com/cd/B19306_01/server.102/b14215/logminer.htm#i1014687 for more information.

Unstable features:
- Reading schema changes from the Archive-Log

Planned features:
- More documentation :)

## Installation
You can install this Kafka Connect component into Kafka Connect using the [Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html). Additionally, you need to download the Oracle 
JDBC driver and put it on the classpath of Logminer Kafka Connect. The JDBC driver can't be included in the Logminer Kafka Connect release
as its license does not allow this. 

The following script will install Logminer Kafka Connect into an existing Kafka Connect installation:
```shell script
wget https://github.com/thake/logminer-kafka-connect/releases/download/0.4.0/thake-logminer-kafka-connect-0.4.0.zip
confluent-hub install ./thake-logminer-kafka-connect-0.4.0.zip --no-prompt
rm ./thake-logminer-kafka-connect-0.4.0.zip
wget https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.7.0.0/ojdbc8-19.7.0.0.jar -o /usr/share/confluent-hub-components/thake-logminer-kafka-connect/lib/ojdbc8-19.7.0.0.jar
``` 
### Docker
If you plan to run Logminer Kafka Connect as a container, you can also have a look at the docker image at https://github.com/thake/logminer-kafka-connect-image 

### Oracle Database Configuration Requirements
In order for Logminer Kafka Connect to work, the database needs to be in ARCHIVELOG mode and Supplemental Logging needs to be 
enabled with all columns. Here are the commands that need to be executed in sqlplus:
```oraclesqlplus
prompt Shutting down database to activate archivelog mode;
shutdown immediate;
startup mount;
alter database archivelog;
prompt Archive log activated.;
alter database add supplemental log data (all) columns;
prompt Activated supplemental logging with all columns.;
prompt Starting up database;
alter database open;
```

## Initial Import
If the start SCN is not set or set to 0, logminer kafka connect will query
the configured tables for an initial import. During the initial import, no
DDL statements should be executed against the database. Otherwise the initial import will
fail.

To support initial import, database flashback queries need to be enabled on the source database.

All rows that are in the table at time of initial import will be treated as "INSERT" changes.

## Change Types
The change types are compatible to change types published by the debezium (https://debezium.io/) project.
Thus it is easy to migrate to the official debezium Oracle plugin ones it reaches a stable state.
The key of the kafka topic will be filled with a struct containing the primary key values of the changed row. 

### Value Struct
The value is a structure having the following fields:
  -  `op`  
      Operation that has been executed.
        - Type: string
        - Possible values:
            - 'r' - Read on initial import
            - 'i' - Insert
            - 'u' - Update
            - 'd' - Delete
  - `before`
     Image of the row before the operation has been executed. Contains all columns.
       - Type: struct
       - Only filled for the following operations:
           - Update
           - Delete
  - `after`
     Image of the row after the operation has been executed. Contains all columns.
       - Type: struct
       - Only filled for the following operations:
           - Insert
           - Read
           - Update
  - `ts_ms`
     Timestamp of import as millis since epoch
       - Type: Long
  - `source`
     Additional information about this change record from the source database. 
       - Type: source

### Source struct       

The following source fields will be provided:
  - `version`  
     Version of this component
       - Type: string
       - Value: '1.0'
  - `connector`  
     Name of this connector.
       - Type: string
       - Value: 'logminer-kafka-connect'
  - `ts_ms`  
     Timestamp of the change in the source database.
       - Type: long
       - Logical Name: org.apache.kafka.connect.data.Timestamp
  - `scn`  
     SCN number of the change. For the initial import this is the scn of the las update to the row.
       - Type: long
  - `txId`  
     Transaction in which the change occurred in the source database. For the initial import, this field is always null.
       - Type: string (optional)       
  - `table`  
     Table in the source database for which the change was recorded.
       - Type: string 
  - `schema`  
     Schema in the source database in which the change was recorded.
      - Type: string
  - `user`  
     The user that triggered the change
       - Type: string (optional)


## Configuration
You can find an example configuration under [logminer-kafka-connect.properties](logminer-kafka-connect.properties).

The following configuration parameter are available:
  - `db.hostname`  
    Database hostname
    
      - Type: string
      - Importance: high

  - `db.name`  
    Logical name of the database. This name will be used as a prefix for
    the topic. You can choose this name as you like.
    
      - Type: string
      - Importance: high

  - `db.port`  
    Database port (usually 1521)
    
      - Type: int
      - Importance: high

  - `db.sid`  
    Database SID
    
      - Type: string
      - Importance: high

  - `db.user`  
    Database user
    
      - Type: string
      - Importance: high

  - `db.user.password`  
    Database password
    
      - Type: string
      - Importance: high

  - `db.logminer.dictionary`  
    Type of logminer dictionary that should be used. 
    Valid values: ONLINE, REDO_LOG
    
      - Type: string
      - Default: ONLINE
      - Importance: low
  - `db.timezone`   
    The timezone in which TIMESTAMP columns (without any timezone information) should be interpreted as. Valid values are all values that can be passed to https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#of-java.lang.String-

      - Type: string
      - Default: UTC
      - Importance: high
  
  - `batch.size`  
    Batch size of rows that should be fetched in one batch
    
      - Type: int
      - Default: 1000
      - Importance: high

  - `start.scn`  
    Start SCN, if set to 0 an initial intake from the tables will be
    performed.
    
      - Type: long
      - Default: 0
      - Importance: high

  - `table.whitelist`  
    Tables that should be monitored, separated by ','. Tables have to be
    specified with schema. Table names are case-sensitive (e.g. if your table name is an unquoted identifier, you'll need to specify it in all caps).
    You can also just specify a schema to indicate
    that all tables within that schema should be monitored. Examples:
    'MY\_USER.TABLE, OTHER\_SCHEMA'.
    
      - Type: string
      - Default: ""
      - Importance: high

  - `db.fetch.size`  
    JDBC result set prefetch size. If not set, it will be defaulted to
    batch.size. The fetch should not be smaller than the batch size.
    
      - Type: int
      - Default: null
      - Importance: medium

  - `db.attempts`  
    Maximum number of attempts to retrieve a valid JDBC connection.
    
      - Type: int
      - Default: 3
      - Importance: low

  - `db.backoff.ms`  
    Backoff time in milliseconds between connection attempts.
    
      - Type: long
      - Default: 10000
      - Importance: low

  - `poll.interval.ms`  
    Positive integer value that specifies the number of milliseconds the
    connector should wait after a polling attempt didn't retrieve any
    results.
    
      - Type: long
      - Default: 2000
      - Importance: low
      
## Limitations
- Due to limitations of Oracle Logminer, it is not possible to track the UPDATE statements to existing records that are implicitly performed whenever a new
not null column with a default value will be added to a table. 

  However you can change the way you add these columns in order to correctly record the UPDATE statements. 
Instead of doing everything in one command, one could separate it into the following steps:
  1. Adding new nullable column
  1. Adding a trigger on insert that automatically inserts the default value for the new nullable column if it is not specified.
  1. Updating the column with the default value for all existing rows
  1. Changing the definition of the column to be NOT NULL with the default value.
  1. Dropping trigger on insert
Performing the DDL in this way would guarantee that the change log will be readable by logminer.