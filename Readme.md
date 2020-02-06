# Logminer Kafka Connect

Logminer Kafka Connect is a Kafka Connect source for Oracle Databases (tested with Oracle 11.2.0.4). 

Changes are extracted from the Archivelog using [Oracle Logminer](https://docs.oracle.com/cd/B19306_01/server.102/b14215/logminer.htm). 

Currently supported features:
- Insert, Update and Delete changes will be tracked on configured tables
- Logminer without "CONTINUOUS_MINE", thus in theory being compatible with Oracle 19c (not tested)

Planned features:
- Initial import of the current state.
- More documentation :)
