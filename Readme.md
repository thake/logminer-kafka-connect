# Logminer Kafka Connect

Logminer Kafka Connect is a CDC Kafka Connect source for Oracle Databases (tested with Oracle 11.2.0.4). 

Changes are extracted from the Archivelog using [Oracle Logminer](https://docs.oracle.com/cd/B19306_01/server.102/b14215/logminer.htm). 

Currently supported features:
- Insert, Update and Delete changes will be tracked on configured tables
- Logminer without "CONTINUOUS_MINE", thus in theory being compatible with Oracle 19c (not tested)
- Initial import of the current table state

Planned features:
- More documentation :)
- Providing primary keys as keys to the Kafka messages.
- Reading schema changes from the Archive-Log. Currently the online catalog is used. See 
https://docs.oracle.com/cd/B19306_01/server.102/b14215/logminer.htm#i1014687 for more information.


##Initial Import
If the start SCN is not set or set to 0, logminer kafka connect will query
the configured tables for an initial import. During the initial import, no
DDL statements should be executed against the database. Otherwise the initial import will
fail.

To support initial import, database flashback queries need to be enabled on the source database.

All rows that are in the table at time of initial import will be treated as "INSERT" changes.

##Change Types
###INSERT
###UPDATE
###DELETE

##Configuration
  - `db.hostname`  
    Database hostname
    
      - Type: string
      - Importance: high

  - `db.name`  
    Database SID
    
      - Type: string
      - Importance: high

  - `db.port`  
    Database port (usually 1521)
    
      - Type: int
      - Importance: high

  - `db.user`  
    Database user
    
      - Type: string
      - Importance: high

  - `db.user.password`  
    Database password
    
      - Type: string
      - Importance: high

  - `batch.size`  
    Batch size of rows that should be fetched in one batch
    
      - Type: int
      - Default: 1000
      - Importance: high

  - `record.prefix`  
    Prefix of the subject record. If you're using an Avro converter,
    this will be the namespace.
    
      - Type: string
      - Default: ""
      - Importance: high

  - `table.whitelist`  
    Tables that should be monitored, separated by ','. Tables have to be
    specified with schema. You can also justspecify a schema to indicate
    that all tables within that schema should be monitored. Examples:
    'MY\_USER.TABLE, OTHER\_SCHEMA'.
    
      - Type: string
      - Default: ""
      - Importance: high

  - `topic.prefix`  
    Prefix for the topic. Each monitored table will be written to a
    separate topic. If you want to changethis behaviour, you can add a
    RegexRouter transform.
    
      - Type: string
      - Default: ""
      - Importance: medium

  - `db.fetch.size`  
    JDBC result set prefetch size. If not set, it will be defaulted to
    batch.size. The fetch should not be smaller than the batch size.
    
      - Type: int
      - Default: null
      - Importance: low

  - `start.scn`  
    Start SCN, if set to 0 an initial intake from the tables will be
    performed.
    
      - Type: long
      - Default: 0
      - Importance: low


