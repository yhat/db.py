# db.py

## What is it?

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
#### `DB()`
#### Saving a profile
#### Connecting from a profile
### Executing Queries
#### From a string
#### From a file
### Finding Tables and Columns
#### Tables 
#### Columns

## TODO
- [x] Switch to newever version of pandas sql api
- [ ] Add database support
    - [x] postgres
    - [ ] redshift
    - [ ] mysql
    - [ ] sqlite
    - [ ] mssql
- [ ] publish examples to nbviewer
- [ ] improve documentation and readme
- [ ] add sample database to distrobution
