CREATE USER SIT IDENTIFIED BY "SIT" DEFAULT TABLESPACE USERS;
alter user system quota unlimited on users;
alter user sit quota unlimited on users;
create table SIT.TEST_TAB
(
    id        NUMBER(8)
        constraint TEST_TAB_pk
            primary key,
    time      TIMESTAMP not null,
    string    VARCHAR2(255),
    "integer" NUMBER(7) not null,
    "long"    NUMBER(14),
    "date"    date      not null,
    big_decimal NUMBER default 0
);
create table SIT.SECOND_TAB
(
    id        NUMBER(8),
    time      TIMESTAMP not null,
    string    VARCHAR2(255),
    "integer" NUMBER(6) not null,
    "long"    NUMBER(14),
    "date"    date      not null,
    big_decimal NUMBER default 0
);
