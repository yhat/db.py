from collections import namedtuple
import glob
import json
import base64
import os
import sys

import pandas as pd
from prettytable import PrettyTable

from queries import mysql as mysql_templates
from queries import postgres as postgres_templates
from queries import sqlite as sqlite_templates


queries_templates = {
    "mysql": mysql_templates,
    "postgres": postgres_templates,
    "sqlite": sqlite_templates,
}

# attempt to import the relevant database libraries
# TODO: maybe add warnings?
try:
    import psycopg2 as pg
except:
    pass

try:
    import MySQLdb
except:
    pass

try:
    import sqlite3 as sqlite
except:
    pass


class Column(object):
    """
    A Columns is an in-memory reference to a column in a particular table. You 
    can use it to do some basic DB exploration and you can also use it to 
    execute simple queries.
    """
    def __init__(self, con, query_templates, table, name, dtype):
        self.con = con
        self.query_templates = query_templates
        self.table = table
        self.name = name
        self.type = dtype

    def __repr__(self):
        tbl = PrettyTable(["Table", "Name", "Type"])
        tbl.add_row([self.table, self.name, self.type])
        return str(tbl)

    def _repr_html_(self):
        tbl = PrettyTable(["Table", "Name", "Type"])
        tbl.add_row([self.table, self.name, self.type])
        return tbl.get_html_string()
    
    def head(self, n=6):
        """
        Returns first n values of your column as a DataFrame. This is executing:
            SELECT
                <name_of_the_column>
            FROM
                <name_of_the_table>
            LIMIT <n>
        
        Parameters
        ----------
        n: int
            number of rows to return

        """
        q = self.query_templates['column']['head'] % (self.name, self.table, n)
        return pd.io.sql.read_sql(q, self.con)[self.name]

    def all(self):
        """
        Returns all unique values as a DataFrame. This is executing:
            SELECT
                DISTINCT
                    <name_of_the_column>
            FROM
                <name_of_the_table>
        """
        q = self.query_templates['column']['all'] % (self.name, self.table)
        return pd.io.sql.read_sql(q, self.con)[self.name]

    def unique(self):
        """
        Returns all unique values as a DataFrame. This is executing:
            SELECT
                DISTINCT
                    <name_of_the_column>
            FROM
                <name_of_the_table>
        """
        q = self.query_templates['column']['unique'] % (self.name, self.table)
        return pd.io.sql.read_sql(q, self.con)[self.name]

    def sample(self, n=10):
        """
        Returns random sample of n rows as a DataFrame. This is executing:
            SELECT
                <name_of_the_column>
            FROM
                <name_of_the_table>
            ORDER BY
                RANDOM()
            LIMIT <n>

        Parameters
        ----------
        n: int
            number of rows to sample
        """
        q = self.query_templates['column']['sample'] % (self.name, self.table, n)
        return pd.io.sql.read_sql(q, self.con)[self.name]

class Table(object):
    """
    A Table is an in-memory reference to a table in a database. You can use it to get more info
    about the columns, schema, etc. of a table and you can also use it to execute queries.
    """
    def __init__(self, con, query_templates, name, cols):
        self.name = name
        self.con = con
        self.query_templates = query_templates
        self._columns = cols
        for col in cols:
            attr = col.name
            if attr in ("name", "con"):
                attr = "_" + col.name
            setattr(self, attr, col)
    
    def __repr__(self):
        tbl = PrettyTable(["Column", "Type"])
        tbl.align["Column"] = "l"
        tbl.align["Type"] = "l"
        for col in self._columns:
            tbl.add_row([col.name, col.type])
        tbl = str(tbl)
        r = tbl.split('\n')[0]
        brk = "+" + "-"*(len(r)-2) + "+"
        title = "|" + self.name.center(len(r)-2) + "|"
        return brk + "\n" + title + "\n" + tbl

    def _repr_html_(self):
        tbl = PrettyTable(["Column", "Type"])
        tbl.align["Column"] = "l"
        tbl.align["Type"] = "l"
        for col in self._columns:
            tbl.add_row([col.name, col.type])
        return tbl.get_html_string()

    def select(self, *args):
        """
        Returns DataFrame of table with arguments selected as columns. This is
        executing:
            SELECT
                <name of column 1>
                , <name of column 2>
                , <name of column 3>
            FROM
                <name_of_the_table>
        
        Parameters
        ----------
        *args: str
            columns to select
        
        Examples
        --------
        >>> db.people.select("name") # select name from people table
        >>> db.people.select("name", "age") # select name and age from people table
        """
        q = self.query_templates['table']['select'] % (", ".join(args), self.name)
        return pd.io.sql.read_sql(q, self.con)
    
    def head(self, n=6):
        """
        Returns first n values of your table as a DataFrame. This is executing:
            SELECT
                *
            FROM
                <name_of_the_table>
            LIMIT <n>
        
        Parameters
        ----------
        n: int
            number of rows to return
        """
        q = self.query_templates['table']['head'] % (self.name, n)
        return pd.io.sql.read_sql(q, self.con)

    def all(self):
        """
        Returns entire table as a DataFrame. This is executing:
            SELECT
                *
            FROM
                <name_of_the_table>
        """
        
        q = self.query_templates['table']['all'] % (self.name)
        return pd.io.sql.read_sql(q, self.con)
    
    def unique(self, *args):
        """
        Returns all unique values as a DataFrame. This is executing:
            SELECT
                DISTINCT
                    <name_of_the_column_1>
                    , <name_of_the_column_2>
                    , <name_of_the_column_3>
                    ...
            FROM
                <name_of_the_table>

        Parameters
        ----------
        *args: columns as strings
        
        Examples
        --------
        >>> db.people.unique("name")
        >>> db.people.unique("name", "age")
        >>> db.people.unique("name", "age", "zipcode")
        """
        if len(args)==0:
            columns = "*"
        else:
            columns = ", ".join(args)
        q = self.query_templates['table']['unique'] % (columns, self.name)
        return pd.io.sql.read_sql(q, self.con)

    def sample(self, n=10):
        """
        Returns random sample of n rows as a DataFrame. This is executing:
            SELECT
                *
            FROM
                <name_of_the_table>
            ORDER BY
                RANDOM()
            LIMIT <n>

        Parameters
        ----------
        n: int
            number of rows to sample
        """
        q = self.query_templates['table']['sample'] % (self.name, n)
        return pd.io.sql.read_sql(q, self.con)


class TableSet(object):
    """
    Set of Tables. Used for displaying search results in terminal/ipython notebook.
    """
    def __init__(self, tables):
        for tbl in tables:
            setattr(self, tbl.name, tbl)
        self.tables = tables
    
    def __getitem__(self, i):
        return self.tables[i]

    def _tablify(self):
        tbl = PrettyTable(["Table", "Columns"])
        tbl.align["Table"] = "l"
        tbl.align["Columns"] = "l"
        for table in self.tables:
            column_names = [col.name for col in table._columns]
            column_names = ", ".join(column_names)
            pretty_column_names = ""
            for i in range(0, len(column_names), 80):
                pretty_column_names += column_names[i:(i+80)] + "\n"
            pretty_column_names = pretty_column_names.strip()
            tbl.add_row([table.name, pretty_column_names])
        return tbl

    def __repr__(self):
        tbl = str(self._tablify())
        return tbl

    def _repr_html_(self):
        return self._tablify().get_html_string()

class ColumnSet(object):
    """
    Set of Columns. Used for displaying search results in terminal/ipython notebook.
    """
    def __init__(self, columns):
        self.columns = columns
    
    def __getitem__(self, i):
        return self.columns[i]

    def _tablify(self):
        tbl = PrettyTable(["Table", "Column Name", "Type"])
        tbl.align["Table"] = "l"
        tbl.align["Column"] = "l"
        tbl.align["Type"] = "l"
        for col in self.columns:
            tbl.add_row([col.table, col.name, col.type])
        return tbl
    
    def __repr__(self):
        tbl = str(self._tablify())
        return tbl

    def _repr_html_(self):
        return self._tablify().get_html_string()

class DB(object):
    """
    Utility for exploring and querying a database.

    Parameters
    ----------
    username: str
        Your username for the database
    password: str
        Your password for the database
    hostname: str
        Hostname your database is running on (i.e. "localhost", "10.20.1.248")
    port: int
        Port the database is running on (defaults to 5432)
    filename: str
        path to sqlite database
    dbname: str
        Name of the database
    profile: str
        Preconfigured database credentials / profile for how you like your queries
    """
    def __init__(self, username=None, password=None, hostname="localhost",
            port=5432, filename=None, dbname=None, dbtype=None, profile="default", exclude_system_tables=True):
        

        if dbtype!="sqlite" and username is None and password is None and hostname=="localhost" and port==5432 and dbname is None:
            self.load_credentials(profile)
        elif dbtype=="sqlite" and filename is None:
            self.load_credentials(profile)
        else:
            self.username = username
            self.password = password
            self.hostname = hostname
            self.port = port
            self.filename = filename
            self.dbname = dbname
            self.dbtype = dbtype

        if self.dbtype is None:
            raise Exception("Database type not specified! Must select one of: postgres, sqlite, mysql, mssql, or redshift")
        self.query_templates = queries_templates.get(self.dbtype).queries

        if dbtype=="postgres" or dbtype=="redshift":
            self.con = pg.connect(user=self.username, password=self.password,
                host=self.hostname, port=self.port, dbname=self.dbname)
            self.cur = self.con.cursor()
        elif dbtype=="sqlite":
            self.con = sqlite.connect(self.filename)
            self.cur = self.con.cursor()
            self._create_sqlite_metatable()
        elif dbtype=="mysql":
            creds = {}
            for arg in ["username", "password", "hostname", "port", "dbname"]:
                if getattr(self, arg):
                    value = getattr(self, arg)
                    if arg=="username":
                        arg = "user"
                    elif arg=="password":
                        arg = "passwd"
                    elif arg=="dbname":
                        arg = "db"
                    elif arg=="hostname":
                        arg = "host"
                    creds[arg] = value
            self.con = MySQLdb.connect(**creds)
            self.cur = self.con.cursor()

        self.tables = TableSet([])
        self.refresh_schema(exclude_system_tables)

    def __repr__(self):
        return "DB[{dbtype}][{hostname}]:{port} > {user}@{dbname}".format(
            dbtype=self.dbtype, hostname=self.hostname, port=self.port, user=self.username, dbname=self.dbname)

    def __delete__(self):
        del self.cur
        del self.con

    def load_credentials(self, profile="default"):
        user = os.path.expanduser("~")
        f = os.path.join(user, ".db.py_" + profile)
        if os.path.exists(f):
            creds = json.loads(base64.decodestring(open(f, 'rb').read()))
            self.username = creds.get('username')
            self.password = creds.get('password')
            self.hostname = creds.get('hostname')
            self.port = creds.get('port')
            self.filename = creds.get('filename')
            self.dbname = creds.get('dbname')
            self.dbtype = creds.get('dbtype')
        else:
            raise Exception("Credentials not configured!")

    def save_credentials(self, profile="default"):
        """
        Save your database credentials so you don't have to save them in script.

        Parameters
        ----------
        profile: str
            (optional) name for your database
        
        >>> db = DB(username="hank", password="foo",
        >>>         hostname="prod.mardukas.com", dbname="bar")
        >>> db.save_credentials(profile="production")
        >>> db = DB(username="hank", password="foo",
        >>>         hostname="staging.mardukas.com", dbname="bar")
        >>> db.save_credentials(profile="staging")
        >>> db = DB(profile="staging")
        """
        if self.filename:
            db_filename = os.path.join(os.getcwd(), self.filename)
        else:
            db_filename = None

        user = os.path.expanduser("~")
        f = os.path.join(user, ".db.py_" + profile)
        creds = {
            "username": self.username,
            "password": self.password,
            "hostname": self.hostname,
            "port": self.port,
            "filename": db_filename,
            "dbname": self.dbname,
            "dbtype": self.dbtype
        }
        with open(f, 'wb') as cfile:
            cfile.write(base64.encodestring(json.dumps(creds)))

    def find_table(self, search):
        """
        Aggresively search through your database's schema for a table.

        Parameters
        -----------
        search: str 
           glob pattern for what you're looking for

        Examples
        ----------
        >>> db.find_table("tmp*") # returns all tables prefixed w/ tmp
        >>> db.find_table("sg_trans*") # returns all tables prefixed w/ sg_trans
        >>> db.find_table("*trans*") # returns all tables containing trans
        >>> db.find_table("*") # returns everything
        """
        tables = []
        for table in self.tables:
            if glob.fnmatch.fnmatch(table.name, search):
                tables.append(table)
        return TableSet(tables)

    def find_column(self, search, data_type=None):
        """
        Aggresively search through your database's schema for a column.

        Parameters
        -----------
        search: str 
           glob pattern for what you're looking for
        data_type: str, list
           (optional) specify which data type(s) you want to return

        Examples
        ----------
        >>> db.find_column("tmp*") # returns all columns prefixed w/ tmp
        >>> db.find_column("sg_trans*") # returns all columns prefixed w/ sg_trans
        >>> db.find_column("*trans*") # returns all columns containing trans
        >>> db.find_column("*trans*", datatype="varchar") # returns all columns containing trans that are varchars
        >>> db.find_column("*trans*", datatype=["varchar", float8]) # returns all columns that are varchars or float8
        >>> db.find_column("*") # returns everything
        """
        if isinstance(data_type, str):
            data_type = [data_type]
        cols = []
        for table in self.tables:
            for col in vars(table):
                if glob.fnmatch.fnmatch(col, search):
                    if data_type and isinstance(getattr(table, col), Column) and getattr(table, col).type not in data_type:
                        continue
                    if isinstance(getattr(table, col), Column):
                        cols.append(getattr(table, col))
        return ColumnSet(cols)

    def query(self, q):
        """
        Query your database with a raw string.

        Parameters
        ----------
        q: str
            Query string to execute
        
        Examples
        --------
        >>> db.query("SELECT * FROM foo LIMIT 100;")
        >>> db.query("SELECT name, sum(1) as cnt FROM foo GROUP BY name;")
        >>> q = '''
            SELECT
                t.name
                t.zipcode
                , avg(t.salary)
                , count(*)
            FROM
                leads t
            GROUP BY
                t.name
                , t.zipcode
            '''
        >>> lead_stats = db.query(q)
        """
        return pd.io.sql.read_sql(q, self.con)

    def query_from_file(self, filename):
        """
        Query your database from a file.

        Parameters
        ----------
        filename: str
            A SQL script
        
        Examples
        --------
        >>> db.query_from_file("myscript.sql")
        """
        return self.query(open(filename).read())

    def _create_sqlite_metatable(self):
        sys.stderr.write("Indexing schema. This will take a second...")
        rows_to_insert = []
        tables = [row[0] for row in self.cur.execute("select name from sqlite_master where type='table';")]
        for table in tables:
            for row in self.cur.execute("pragma table_info(%s)" % table):
                rows_to_insert.append((table, row[1], row[2]))
        self.cur.execute("drop table if exists tmp_dbpy_schema;")
        self.cur.execute("create temp table tmp_dbpy_schema(table_name varchar, column_name varchar, data_type varchar);")
        for row in rows_to_insert:
            self.cur.execute("insert into tmp_dbpy_schema(table_name, column_name, data_type) values('%s', '%s', '%s');" % row)
        self.con.commit()
        sys.stderr.write("finished!\n")
    
    def refresh_schema(self, exclude_system_tables=True):
        """
        Pulls your database's schema again and looks for any new tables and
        columns.
        """

        if exclude_system_tables==True:
            q = self.query_templates['system']['schema_no_system']
        else:
            q = self.query_templates['system']['schema_with_system']

        tables = set()
        self.cur.execute(q)
        cols = []
        tables = {}
        for (table_name, column_name, data_type)in self.cur:
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(Column(self.con, self.query_templates, table_name, column_name, data_type))
        self.tables = TableSet([Table(self.con, self.query_templates, t, tables[t]) for t in sorted(tables.keys())])


    def shell(self):
        pass
