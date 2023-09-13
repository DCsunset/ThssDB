# ThssDB

A SQL database written in Java from scratch for a database course.

## Usage

Run `maven clean install` first to install dependencies.

The following java program can be run directly:

* Server: `server/ThssDB.java`
* Interactive client: `client/OurClient.java`
* Client test: `client/Client.java`

## Manual

After starting the server and the client,
a command prompt will be available at the client.

### Authentication

After running the client, login is required before using the datbase.
You can use the following commands for authentication:

```sql
-- login
connect <username> <password>
-- logout
disconnect
```

A default user is created with the username `username` and password `password`.


### User Management

After login, you can manage the user using the following commands:

```sql
-- create a user
create user <username> identified by <password>;
-- change user password
alter user <username> set password <password>;
-- delete a user
drop user <username>;
```

### SQL Queries

#### Database Management

```sql
-- create a database
-- return error if the database already exists
CREATE DATABASE dbName;

-- switch current datbase
-- return error if the database doesn't exist
USE dbName;

-- drop database
-- return error if the database doesn't exist
DROP DATABASE dbName;
```


#### Table Creation

To create a table in the current database:


```sql
-- create a table in current database
-- return error if the table already exists
CREATE TABLE tableName(attrName1 Type1, attrName2 Type2, ..., attrNameN TypeN NOT NULL, PRIMARY KEY(attrName1));
```


This database supports 5 types: `String(n)`, `Int`, `Long`, `Float`, `Double`.
You can use one of the above types for each attribute in the table.

Note: `primary key(attr)` must be used on an existing attribute,
and `not null` can be appended after any attribute definition.


#### Table Management

```sql
-- show metadata of the table
-- return error if the table doesn't exist
SHOW TABLE tableName;
-- drop a table
-- return error if the table doesn't exist
DROP TABLE tableName;
```

#### Insert

```sql
INSERT INTO tableName(attrName1, attrName2,…, attrNameN) VALUES (attrValue1, attrValue2,…, attrValueN);
```

The attribute list after the table is optional.
if it's not specified, you need to provide values for all attributes.
Otherwise, you can just provide valus corresponding to the specified attribute names (other attributes will be `null`).
The `not null` constraint will be checked if any attribute has it defined.

#### Update

```sql
UPDATE tableName SET attrName = attrValue WHERE <condition>;
```

The condition supports multiple expressions joined by `&&` or `||` operators,
attribute names and constant expressions are supported in the condition clause.
Constant expression

For example：

```sql
update student set tot_cred = 25*5 where tot_cred >= 25*(6-1) && tot_cred <= 127;
```

#### Select

For a single table:

```sql
SELECT attrName1, attrName2, … attrNameN FROM tableName [ WHERE  <condition> ]
```

For multiple tables

```sql
SELECT tableName1.AttrName1, tableName1.AttrName2…, tableName2.AttrName1, tableName2.AttrName2,… FROM tableName1 JOIN tableName2  ON  tableName1.attrName1 = tableName2.attrName2 [ WHERE <condition> ]
```

The where clause is optional in the above queries.
If only one `*` is used for the attribute name,
it will represent all the attributes.

When used with multiple tables,
the tableName can't be ommited.

Exmaple queries:

```sql
create table grade(id String(10), score Int);
insert into grade values('1', 80);
insert into grade values('2', 90);
insert into grade values('3', 99);
insert into grade values('4', 100);
create table person(id String(10), name String(10));
insert into person values('1', 'A');
insert into person values('2', 'B');
insert into person values('3', 'C');
insert into person values('4', 'D');
create table info(name String(10), birthday String(20));
insert into info values('A', '2000-1-1');
insert into info values('B', '2000-1-2');
insert into info values('C', '2000-1-3');
insert into info values('D', '2000-1-4');
select * from person join info join grade on grade.id = person.id && person.name = info.name;
```


### Transactions & Checkpoints

The following commands are supported:

```sql
-- begin a transaction
begin transaction;
-- commit the current transaction
commit;
-- roll back current transaction
rollback;
-- set checkpoint
checkpoint;
```

When a client begins a transaction,
locks will be required for involved tables.
Other clients must wait for the transaction to finish
before operating on the same table.


The `checkpoint` command will cause the server to write checkpoint to the log and persist the data.
When recovering the data, it can start from the last checkpoing directly in the log,
which will be more efficient compared to replaying all the logs.

In the mean time, `checkpoint` command will be periodically executed in another thread as well
to improve the efficiency for crash recovery.


## Copyright Notice

All rights reserved.

