# db.py

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

## TODO
- [ ] Switch to newever version of pandas sql api
- [ ] Add database support
    - [x] postgres
    - [ ] redshift
    - [ ] mysql
    - [ ] sqlite
    - [ ] mssql
- [ ] publish examples to nbviewer
- [ ] improve documentation and readme
- [ ] add sample database to distrobution
