# How to Add a Database to `db.py`

## Before you start
There are basically 3 types of relational databases connections:

- Postgres and Postgres variants (Redshift, Greenplum, Vertica, etc.)
- MySQL and MySQL variants (MariaDB, MemSQL, etc.)
- ODBC (MS SQL, Oracle, etc.)

There are versions of each of these already in `db.py` so be sure to check
if the database you're adding is compatible with one of these. It might save
you a lot of work.

## Databases currently supported

- Postgres
- SQLite
- MySQL
- MS SQL Server
- Redshift

## Databases Requested

- Oracle
- Teradata

## Checklist
For this example we'll be adding a database called `foosql`.

### Queries
You'll need to port the required queries into whatever flavor of SQL your 
database uses. If you're even somewhat familiar with SQL, this shouldn't
be too difficult since you can go off of one of other databases (they're almost
all the same). Here's a checklist of what you'll need to do:

- [ ] column
    - [ ] head: select first N values of column 
    - [ ] unique: select distinct values of column
    - [ ] all: select all values of column
    - [ ] sample: randomly select N values of column
- [ ] table
    - [ ] head: select first N rows of table 
    - [ ] select: select specified columns from table
    - [ ] unique: select distinct columns specified from table
    - [ ] all: select all rows from table
    - [ ] sample: randomly select N rows of table
- [ ] system
    - [ ] schema_no_system
    - [ ] schema_with_system
    - [ ] schema_specified
    - [ ] foreign_keys_for_table
    - [ ] foreign_keys_for_column
    - [ ] ref_keys_for_table

Create a file called `foosql.py` and put it in the [`queries`](https://github.com/yhat/db.py/tree/master/db/queries) 
directory. Once you've written the necessary queries, add an import statement 
to the `db.py` file. It will look something like this:

```python
from .queries import foosql as foosql_templates
```

You'll also need to add your templates to the `queries_templates`:

```python
queries_templates = {
  "mysql": mysql_templates,
  "postgres": postgres_templates,
  "redshift": postgres_templates,
  "sqlite": sqlite_templates,
  "mssql": mssql_templates,
  "foosql": foosql_templates
}
```

### Connecting
__Client Library__
`db.py` relies on the [PEP 249](http://legacy.python.org/dev/peps/pep-0249/) 
spec for connecting and querying. If the database you're using requires a new
library for connecting, be sure it is PEP 249 compliant.

If you do need to use a new library, you can add it at the top of [`db.py`](https://github.com/yhat/db.py/blob/master/db/db.py#L33-L57). 
Just make sure to put a try/catch around it.

__Alterations to `DB`__
You'll need to update the `DB` class to handle default connection parameters 
for your database. For example, if `foosql` defaulted to running on port 6789,
I would need to update the `if port is None...` statement by adding a condition
for `foosql`:

```python
if port is None:
    if dbtype=="postgres":
        port = 5432
    ...
    elif dbtype=="foosql":
        port = 6789
```

There will also be some docstrings to update as well. Please add examples 
accordingly.

Lastly you'll need to actually make the connection. This is where you'll 
actually invoke the client library. For example, adding in `foosql` would 
look like this:

```python
if self.dbtype=="postgres" or self.dbtype=="redshift":
    if not HAS_PG:
        raise Exception("Couldn't find psycopg2 library. Please ensure it is installed")
    self.con = pg.connect(user=self.username, password=self.password,
                          host=self.hostname, port=self.port, dbname=self.dbname)
    self.cur = self.con.cursor()
elif self.dbtype=="foosql":
    if not HAS_FOOSQL:
        raise Exception("Couldn't find foosql library. Please ensure it is installed")
    self.con = pg.connect(user=self.username, password=self.password,
                          host=self.hostname, port=self.port, dbname=self.dbname)
    self.cur = self.con.cursor()
```

## Wrap Up
That's pretty much it. It's actually pretty easy to add databases. If you're 
having troubles, visit the [issues page](https://github.com/yhat/db.py/issues) 
on github.
