CREATE USER SIT IDENTIFIED BY "SIT" DEFAULT TABLESPACE USERS;
alter user system quota unlimited on users;
alter user sit quota unlimited on users;