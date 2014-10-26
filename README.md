# db.py

## What is it?
`db.py` is an easier way to interact with your databases. It makes it easier to explore tables, columns, views, etc. It puts the emphasis on user interaction, information display, and providing easy to use helper functions.

`db.py` uses [`pandas`](http://pandas.pydata.org/) to manage data, so if you're already using `pandas`, `db.py` should feel pretty natural. It's also fully compatible with the IPython Notebook, so not only is `db.py` extremely functional, it's also pretty.

Execute queries
```python
>>> db.query_from_file("myscript.sql")
       _id                    datetime           user_id  n
0  1290000  10/Jun/2014:18:21:27 +0000  0000015b37cd0964  1
1  9120009  23/Jun/2014:02:11:21 +0000  00006e01a6419822  1
2  1683874  23/Jun/2014:02:11:48 +0000  00006e01a6419822  2
3  2562153  23/Jun/2014:02:12:57 +0000  00006e01a6419822  3
4   393019  14/Jun/2014:16:05:18 +0000  000099d569e3a216  1
5  3542568  14/Jun/2014:16:06:02 +0000  000099d569e3a216  2
```

Fully compatible with predictive type

```python
db.tables.
db.tables.administrable_role_authorizations      db.tables.domain_udt_usage                       db.tables.referential_constraints                db.tables.tables
db.tables.applicable_roles                       db.tables.domains                                db.tables.role_column_grants                     db.tables.tmp_mt_model
db.tables.attributes                             db.tables.element_types                          db.tables.role_routine_grants                    db.tables.tracking
db.tables.character_sets                         db.tables.enabled_roles                          db.tables.role_table_grants                      db.tables.triggered_update_columns
db.tables.check_constraint_routine_usage         db.tables.foreign_data_wrapper_options           db.tables.role_udt_grants                        db.tables.triggers
db.tables.check_constraints                      db.tables.foreign_data_wrappers                  db.tables.role_usage_grants                      db.tables.udt_privileges
db.tables.collation_character_set_applicability  db.tables.foreign_server_options                 db.tables.routine_privileges                     db.tables.usage_privileges
db.tables.collations                             db.tables.foreign_servers                        db.tables.routines                               db.tables.user_defined_types
db.tables.column_domain_usage                    db.tables.foreign_table_options                  db.tables.schemata                               db.tables.user_mapping_options
db.tables.column_options                         db.tables.foreign_tables                         db.tables.sequences                              db.tables.user_mappings
db.tables.column_privileges                      db.tables.ga_data                                db.tables.sql_features                           db.tables.users
db.tables.column_udt_usage                       db.tables.index                                  db.tables.sql_implementation_info                db.tables.view_column_usage
```

Friendly displays
```python
>>> db.tables.mt_s3_logs
+---------------------------------+
|            mt_s3_logs           |
+-----------------------+---------+
| Column                | Type    |
+-----------------------+---------+
| bucket_owner          | varchar |
| bucket                | varchar |
| datetime              | varchar |
| ip                    | varchar |
| requestor_id          | varchar |
| request_id            | varchar |
| operation             | varchar |
| key                   | varchar |
| http_method_uri_proto | varchar |
| http_status           | varchar |
| s3_error              | varchar |
| bytes_sent            | varchar |
| object_size           | varchar |
| total_time            | varchar |
| turn_around_time      | varchar |
| referer               | varchar |
| user_agent            | varchar |
| _id                   | int8    |
+-----------------------+---------+
```

Directly integrated with `pandas`
```python
>>> db.tables.mt_s3_logs.head()
   _id                                       bucket_owner     bucket
0    1  cf77fe33107978c68ebf91e44d101ec99ec75c3cba670e...  moontower
1    2  cf77fe33107978c68ebf91e44d101ec99ec75c3cba670e...  moontower
2    3  cf77fe33107978c68ebf91e44d101ec99ec75c3cba670e...  moontower
3    4  cf77fe33107978c68ebf91e44d101ec99ec75c3cba670e...  moontower
4    5  cf77fe33107978c68ebf91e44d101ec99ec75c3cba670e...  moontower
5    6  cf77fe33107978c68ebf91e44d101ec99ec75c3cba670e...  moontower
```

Search your schema
```python
In [10]: db.find_column("*_id*", data_type=["int4", "int8"])
Out[10]:
+------------------+-------------+------+
| Table            | Column Name | Type |
+------------------+-------------+------+
| ga_data          |     _id     | int4 |
| jobs             |     _id     | int4 |
| mt_s3_logs       |     _id     | int8 |
| mt_s3_logs_users |     _id     | int8 |
| tmp_mt_model     |     _id     | int8 |
| tracking         |     _id     | int4 |
| users            |     _id     | int4 |
+------------------+-------------+------+
```

## Quickstart

### Installation
```bash
$ pip install db.py
```

### Getting Started
```python
>>> from db import DB
>>> db = DB(username="greg", password="secret", hostname="localhost",
            dbtype="postgres")
>>> db.save_credentials(profile="local")
```

## How To

### Connecting to a Database
#### The `DB()` object
__Arguments__

- *username*: your username
- *password*: your password
- *hostname*: hostname of the database (i.e. `localhost`, `dw.mardukas.com`, `ec2-54-191-289-254.us-west-2.compute.amazonaws.com`)
- *port*: port the database is running on (i.e. 5432)
- *dbname*: name of the database (i.e. `hanksdb`)
- *filename*: path to sqlite database (i.e. `baseball-archive-2012.sqlite`, `employees.db`)
- *dbtype*: type of database you're connecting to (postgres, mysql, sqlite, redshfit)
- *profile*: name of the profile you want to use to connect. using this negates the need to specify any other arguments
- *exclude_system_tables*: whether or not to load schema information for internal tables. for example, postgres has a bunch of tables prefixed with `pg_` that you probably don't actually care about. on the other had if you're administrating a database, you might want to query these tables

```python
>>> from db import DB
>>> db = DB(username="greg", password="secret", hostname="localhost",
            dbtype="postgres")
```
#### Saving a profile
```python
>>> from db import DB
>>> db = DB(username="greg", password="secret", hostname="localhost",
            dbtype="postgres")
>>> db.save_credentials() # this will save to "default"
>>> db.save_credentials(profile="local_pg")
```
#### Connecting from a profile
```python
>>> from db import DB
>>> db = DB() # this loads "default" profile
>>> db = DB(profile="local_pg")
```
### Executing Queries
#### From a string
```python
>>> db.query("select * from foo;")
>>> df = db.query("select * from bar;")
```
#### From a file
```python
>>> db.query_from_file("myscript.sql")
>>> df = db.query_from_file("myscript.sql")
```
### Searching for Tables and Columns
#### Tables
```python
>>> db.find_table("*mt*")
+------------------+----------------------------------------------------------------------------------+
| Table            | Columns                                                                          |
+------------------+----------------------------------------------------------------------------------+
| mt_s3_logs       | bucket_owner, bucket, datetime, ip, requestor_id, request_id, operation, key, ht |
|                  | tp_method_uri_proto, http_status, s3_error, bytes_sent, object_size, total_time, |
|                  |  turn_around_time, referer, user_agent, _id                                      |
| mt_s3_logs_users | _id, user_id                                                                     |
| tmp_mt_model     | _id, datetime, user_id, n, key, previous_key, tdiff, same_session                |
+------------------+----------------------------------------------------------------------------------+
>>> results = db.find_table("tmp*") # returns all tables prefixed w/ tmp
>>> results = db.find_table("sg_trans*") # returns all tables prefixed w/ sg_trans
>>> results = db.find_table("*trans*") # returns all tables containing trans
>>> results = db.find_table("*") # returns everythin
```
#### Columns
```python
>>> db.find_column("_id")
+------------------+-------------+------+
| Table            | Column Name | Type |
+------------------+-------------+------+
| ga_data          |     _id     | int4 |
| jobs             |     _id     | int4 |
| mt_s3_logs       |     _id     | int8 |
| mt_s3_logs_users |     _id     | int8 |
| tmp_mt_model     |     _id     | int8 |
| tracking         |     _id     | int4 |
| users            |     _id     | int4 |
+------------------+-------------+------+
>>> results = db.find_column("tmp*") # returns all columns prefixed w/ tmp
>>> results = db.find_column("sg_trans*") # returns all columns prefixed w/ sg_trans
>>> results = db.find_column("*trans*") # returns all columns containing trans
>>> results = db.find_column("*trans*", datatype="varchar") # returns all columns containing trans that are varchars
>>> results = db.find_column("*trans*", datatype=["varchar", float8]) # returns all columns that are varchars or float8
>>> results = db.find_column("*") # returns everything
```

## TODO
- [x] Switch to newever version of pandas sql api
- [ ] Add database support
    - [x] postgres
    - [x] sqlite
    - [ ] redshift
    - [x] mysql
    - [ ] mssql
- [ ] publish examples to nbviewer
- [x] improve documentation and readme
- [ ] add sample database to distrobution
