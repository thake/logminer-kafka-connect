CREATE USER SIT IDENTIFIED BY "SIT" DEFAULT TABLESPACE USERS;
alter user system quota unlimited on users;
alter user sit quota unlimited on users;
ALTER DATABASE SET TIME_ZONE = 'Europe/Berlin';
create table SIT.TIME_TEST
(
    id        NUMBER(8)
        constraint TIME_TEST_pk
            primary key,
    time      TIMESTAMP not null,
    time_with_time_zone TIMESTAMP WITH TIME ZONE not null,
    time_with_local_time_zone TIMESTAMP WITH LOCAL TIME ZONE not null
);