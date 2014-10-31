from collections import namedtuple
import glob
import uuid
import json
import base64
import math
import re
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

try:
    import pyodbc
except:
    pass


class Column(object):
    """
    A Columns is an in-memory reference to a column in a particular table. You
    can use it to do some basic DB exploration and you can also use it to
    execute simple queries.
    """
    def __init__(self, con, query_templates, table, name, dtype):
        self._con = con
        self._query_templates = query_templates
        self.table = table
        self.name = name
        self.type = dtype

        self.foreign_keys = []
        self.ref_keys = []

    def __repr__(self):
        tbl = PrettyTable(["Table", "Name", "Type", "Foreign Keys",
                           "Reference Keys"])
        tbl.add_row([self.table, self.name, self.type, self._str_foreign_keys(),
                     self._str_ref_keys()])
        return str(tbl)

    def __str__(self):
        return "Column(%s)<%d>" % (self.name, self.__hash__())

    def _repr_html_(self):
        tbl = PrettyTable(["Table", "Name", "Type"])
        tbl.add_row([self.table, self.name, self.type])
        return tbl.get_html_string()

    def _str_foreign_keys(self):
        keys = []
        for col in self.foreign_keys:
            keys.append("%s.%s" % (col.table, col.name))
        return ", ".join(keys)

    def _str_ref_keys(self):
        keys = []
        for col in self.ref_keys:
            keys.append("%s.%s" % (col.table, col.name))
        return ", ".join(keys)

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

        Examples
        --------
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> db.tables.Customer.City.head()
        0    Sao Jose dos Campos
        1              Stuttgart
        2               Montreal
        3                   Oslo
        4                 Prague
        5                 Prague
        Name: City, dtype: object
        >>> db.tables.Customer.City.head(2)
        0    Sao Jose dos Campos
        1              Stuttgart
        Name: City, dtype: object
        """
        q = self._query_templates['column']['head'] % (self.name, self.table, n)
        return pd.io.sql.read_sql(q, self._con)[self.name]

    def all(self):
        """
        Returns all unique values as a DataFrame. This is executing:
            SELECT
                DISTINCT
                    <name_of_the_column>
            FROM
                <name_of_the_table>

        Examples
        --------
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> db.tables.Customer.Email.all()
        0              luisg@embraer.com.br
        1             leonekohler@surfeu.de
        2               ftremblay@gmail.com
        3             bjorn.hansen@yahoo.no
        4          frantisekw@jetbrains.com
        5                   hholy@gmail.com
        6            astrid.gruber@apple.at
        7             daan_peeters@apple.be
        8             kara.nielsen@jubii.dk
        9          eduardo@woodstock.com.br
        10                 alero@uol.com.br
        11    roberto.almeida@riotur.gov.br
        ...
        >>> df = db.tables.Customer.Email.all()
        >>> len(df)
            59
        """
        q = self._query_templates['column']['all'] % (self.name, self.table)
        return pd.io.sql.read_sql(q, self._con)[self.name]

    def unique(self):
        """
        Returns all unique values as a DataFrame. This is executing:
            SELECT
                DISTINCT
                    <name_of_the_column>
            FROM
                <name_of_the_table>

        Examples
        --------
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> db.tables.Customer.FirstName.unique()
        0          Luis
        1        Leonie
        2      Francois
        3         Bjorn
        4     Frantisek
        5        Helena
        6        Astrid
        7          Daan
        8          Kara
        9       Eduardo
        10    Alexandre
        ...
        >>> len(db.tables.Customer.LastName.unique())
        """
        q = self._query_templates['column']['unique'] % (self.name, self.table)
        return pd.io.sql.read_sql(q, self._con)[self.name]

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

        Examples
        --------
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> db.tables.Artist.Name.sample(10)
        0                     Julian Bream
        1                         Godsmack
        2                             Lost
        3                         Fretwork
        4            Pedro Luis E A Parede
        5            Philip Glass Ensemble
        6                      Marvin Gaye
        7                        Metallica
        8                Alanis Morissette
        9    Santana Feat. The Project G&B
        Name: Name, dtype: object
        """
        q = self._query_templates['column']['sample'] % (self.name, self.table, n)
        return pd.io.sql.read_sql(q, self._con)[self.name]

class Table(object):
    """
    A Table is an in-memory reference to a table in a database. You can use it to get more info
    about the columns, schema, etc. of a table and you can also use it to execute queries.
    """
    def __init__(self, con, query_templates, name, cols):
        self.name = name
        self._con = con
        self._cur = con.cursor()
        self._query_templates = query_templates
        self.foreign_keys = []
        self.ref_keys = []

        self._columns = cols
        for col in cols:
            attr = col.name
            if attr in ("name", "con"):
                attr = "_" + col.name
            setattr(self, attr, col)

        self._cur.execute(self._query_templates['system']['foreign_keys_for_table'] % (self.name))
        for (column_name, foreign_table, foreign_column) in self._cur:
            col = getattr(self, column_name)
            foreign_key = Column(con, queries_templates, foreign_table, foreign_column, col.type)
            self.foreign_keys.append(foreign_key)
            col.foreign_keys.append(foreign_key)
            setattr(self, column_name, col)

        self.foreign_keys = ColumnSet(self.foreign_keys)

        self._cur.execute(self._query_templates['system']['ref_keys_for_table'] % (self.name))
        for (column_name, ref_table, ref_column) in self._cur:
            col = getattr(self, column_name)
            ref_key = Column(con, queries_templates, ref_table, ref_column, col.type)
            self.ref_keys.append(ref_key)
            col.ref_keys.append(ref_key)
            setattr(self, column_name, col)

        self.ref_keys = ColumnSet(self.ref_keys)

    def _tablify(self):
        tbl = PrettyTable(["Column", "Type", "Foreign Keys", "Reference Keys"])
        tbl.align["Column"] = "l"
        tbl.align["Type"] = "l"
        tbl.align["Foreign Keys"] = "l"
        tbl.align["Reference Keys"] = "l"
        for col in self._columns:
            tbl.add_row([col.name, col.type, col._str_foreign_keys(), col._str_ref_keys()])
        return tbl

    def __repr__(self):
        tbl = str(self._tablify())
        r = tbl.split('\n')[0]
        brk = "+" + "-"*(len(r)-2) + "+"
        title = "|" + self.name.center(len(r)-2) + "|"
        return brk + "\n" + title + "\n" + tbl

    def __str__(self):
        return "Table(%s)<%d>" % (self.name, self.__hash__())

    def _repr_html_(self):
        return self._tablify().get_html_string()

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
        >>> from db import DemoDB
        >>> db = DemoDB()
        # select name from the Track table
        >>> db.tables.Track.select("Name")
                                                           Name
        0               For Those About To Rock (We Salute You)
        1                                     Balls to the Wall
        2                                       Fast As a Shark
        3                                     Restless and Wild
        4                                  Princess of the Dawn
        5                                 Put The Finger On You
        6                                       Let's Get It Up
        7                                      Inject The Venom
        8                                            Snowballed
        9                                            Evil Walks
        ...
        # select name & composer from the Track table
        >>> df = db.tables.Track.select("Name", "Composer")
        """
        q = self._query_templates['table']['select'] % (", ".join(args), self.name)
        return pd.io.sql.read_sql(q, self._con)

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

        Examples
        --------
        >>> from db import DemoDB
        >>> db = DemoDB()
        # select name from the Track table
        >>> db.tables.Track.head()
           TrackId                                     Name  AlbumId  MediaTypeId  \
        0        1  For Those About To Rock (We Salute You)        1            1
        1        2                        Balls to the Wall        2            2
        2        3                          Fast As a Shark        3            2
        3        4                        Restless and Wild        3            2
        4        5                     Princess of the Dawn        3            2
        5        6                    Put The Finger On You        1            1

           GenreId                                           Composer  Milliseconds  \
        0        1          Angus Young, Malcolm Young, Brian Johnson        343719
        1        1                                               None        342562
        2        1  F. Baltes, S. Kaufman, U. Dirkscneider & W. Ho...        230619
        3        1  F. Baltes, R.A. Smith-Diesel, S. Kaufman, U. D...        252051
        4        1                         Deaffy & R.A. Smith-Diesel        375418
        5        1          Angus Young, Malcolm Young, Brian Johnson        205662

              Bytes  UnitPrice
        0  11170334       0.99
        1   5510424       0.99
        2   3990994       0.99
        3   4331779       0.99
        4   6290521       0.99
        5   6713451       0.99
        >>> db.tables.Track.head(1)
           TrackId                                     Name  AlbumId  MediaTypeId  \
        0        1  For Those About To Rock (We Salute You)        1            1

           GenreId                                   Composer  Milliseconds     Bytes  \
        0        1  Angus Young, Malcolm Young, Brian Johnson        343719  11170334

           UnitPrice
        0       0.99
        """
        q = self._query_templates['table']['head'] % (self.name, n)
        return pd.io.sql.read_sql(q, self._con)

    def all(self):
        """
        Returns entire table as a DataFrame. This is executing:
            SELECT
                *
            FROM
                <name_of_the_table>

        Examples
        --------
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> len(db.tables.Track.all())
            3503
        >>> df = db.tables.Track.all()
        """

        q = self._query_templates['table']['all'] % (self.name)
        return pd.io.sql.read_sql(q, self._con)

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
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> db.tables.Track.unique("GenreId")
                GenreId
            0         1
            1         2
            2         3
            3         4
            4         5
            5         6
            6         7
            7         8
            8         9
            9        10
            10       11
            11       12
            12       13
            13       14
            14       15
            15       16
            16       17
            17       18
            18       19
            19       20
            20       21
            21       22
            22       23
            23       24
            24       25
        >>> len(db.tables.Track.unique("GenreId", "MediaTypeId"))
            38
        """
        if len(args)==0:
            columns = "*"
        else:
            columns = ", ".join(args)
        q = self._query_templates['table']['unique'] % (columns, self.name)
        return pd.io.sql.read_sql(q, self._con)

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

        Examples
        --------
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> db.tables.Track.sample(10)
           TrackId                                               Name  AlbumId  \
        0      274                                      Samba Makossa       25
        1     1971                                Girls, Girls, Girls      162
        2      843                                               Otay       68
        3     3498  Concerto for Violin, Strings and Continuo in G...      342
        4     3004                        Pride (In The Name Of Love)      238
        5     2938                                      Beautiful Day      233
        6     2023                          O Braco Da Minha Guitarra      165
        7     1920                                            Caxanga      158
        8     3037                                       The Wanderer      240
        9     1487                           Third Stone From The Sun      120

           MediaTypeId  GenreId                                           Composer  \
        0            1        7                                               None
        1            1        3                     Mick Mars/Nikki Sixx/Tommy Lee
        2            1        2  John Scofield, Robert Aries, Milton Chambers a...
        3            4       24                           Pietro Antonio Locatelli
        4            1        1                                                 U2
        5            1        1         Adam Clayton, Bono, Larry Mullen, The Edge
        6            1        1                                               None
        7            1        7                  Milton Nascimento, Fernando Brant
        8            1        1                                           U2; Bono
        9            1        1                                       Jimi Hendrix

           Milliseconds     Bytes  UnitPrice
        0        271856   9095410       0.99
        1        270288   8874814       0.99
        2        423653  14176083       0.99
        3        493573  16454937       0.99
        4        230243   7549085       0.99
        5        248163   8056723       0.99
        6        258351   8469531       0.99
        7        245551   8144179       0.99
        8        283951   9258717       0.99
        9        404453  13186975       0.99
        """
        q = self._query_templates['table']['sample'] % (self.name, n)
        return pd.io.sql.read_sql(q, self._con)


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
    limit: int, None
        Default number of records to return in a query. This is used by the DB.query
        method. You can override it by adding limit={X} to the `query` method, or
        by passing an argument to `DB()`. None indicates that there will be no
        limit (That's right, you'll be limitless. Bradley Cooper style.)

    Examples
    --------
    >>> from db import DB
    >>> db = DB(username="kermit", password="ilikeflies", hostname="themuppets.com",
                port=5432, dbname="muppets", dbtype="postgres")
    >>> db = DB(username="fozzybear", password="wakawakawaka", hostname="ec2.523.24.131",
                port=5432, dbname="muppets_redshift", dbtype="redshift")
    >>> db = DB(username="dev", hostname="localhost",
                port=5432, dbname="devdb", dbtype="postgres")
    >>> db = DB(username="root", hostname="localhost", dbname="employees", dbtype="mysql")
    >>> db = DB(filename="/path/to/mydb.sqlite", dbtype="sqlite")
    """
    def __init__(self, username=None, password=None, hostname="localhost",
            port=None, filename=None, dbname=None, dbtype=None, profile="default",
            exclude_system_tables=True, limit=1000):

        if port is None:
            if dbtype in ("postgres", "redshift"):
                port = 5432
            elif dbtype=="mysql":
                port = 3306
            elif dbtype=="sqlite":
                port = None
            elif dbtype=="mssql":
                port = None
            else:
                raise Exception("Database type not specified! Must select one of: postgres, sqlite, mysql, mssql, or redshift")

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
            self.limit = limit

        if self.dbtype is None:
            raise Exception("Database type not specified! Must select one of: postgres, sqlite, mysql, mssql, or redshift")
        self._query_templates = queries_templates.get(self.dbtype).queries

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
        elif dbtype=="mssql":
            raise Exception("MS SQL not yet suppported")
            # self.con = pyodbc.connect

        self.tables = TableSet([])
        self.refresh_schema(exclude_system_tables)

    def __str__(self):
        return "DB[{dbtype}][{hostname}]:{port} > {user}@{dbname}".format(
            dbtype=self.dbtype, hostname=self.hostname, port=self.port, user=self.username, dbname=self.dbname)

    def __repr__(self):
        return self.__str__()

    def __delete__(self):
        del self.cur
        del self.con

    def load_credentials(self, profile="default"):
        """
        Loads crentials for a given profile. Profiles are stored in
        ~/.db.py_{profile_name} and are a base64 encoded JSON file. This is not
        to say this a secure way to store sensitive data, but it will probably
        stop your little sister from stealing your passwords.

        Parameters
        ----------
        profile: str
            (optional) identifier/name for your database (i.e. "dw", "prod")
        """
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
            self.limit = creds.get('limit')
        else:
            raise Exception("Credentials not configured!")

    def save_credentials(self, profile="default"):
        """
        Save your database credentials so you don't have to save them in script.

        Parameters
        ----------
        profile: str
            (optional) identifier/name for your database (i.e. "dw", "prod")

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
            "limit": self.limit,
        }
        with open(f, 'wb') as credentials_file:
            credentials_file.write(base64.encodestring(json.dumps(creds)))

    def find_table(self, search):
        """
        Aggresively search through your database's schema for a table.

        Parameters
        -----------
        search: str
           glob pattern for what you're looking for

        Examples
        ----------
        >>> db.find_table("A*")
            +--------+--------------------------+
            | Table  | Columns                  |
            +--------+--------------------------+
            | Album  | AlbumId, Title, ArtistId |
            | Artist | ArtistId, Name           |
            +--------+--------------------------+
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

    def _assign_limit(self, q, limit=1000):
        # postgres, mysql, & sqlite
        if self.dbtype in ["postgres", "redshift", "sqlite", "mysql"]:
            if limit:
                q = q.rstrip(";")
                q = "select * from (%s) q limit %d" % (q, limit)
            return q
        # mssql
        else:
            if limit:
                q = "select top %d from (%s) q" % (limit, q)
                return q

    def query(self, q, limit=None):
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
        if limit==False:
            pass
        else:
            q = self._assign_limit(q, limit)
        return pd.io.sql.read_sql(q, self.con)

    def query_from_file(self, filename, limit=None):
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
        return self.query(open(filename).read(), limit)

    def _create_sqlite_metatable(self):
        sys.stderr.write("Indexing schema. This will take a second...")
        rows_to_insert = []
        tables = [row[0] for row in self.cur.execute("select name from sqlite_master where type='table';")]
        for table in tables:
            for row in self.cur.execute("pragma table_info(%s)" % table):
                rows_to_insert.append((table, row[1], row[2]))
        # grep for table and column names
        self.cur.execute("drop table if exists tmp_dbpy_schema;")
        self.cur.execute("create temp table tmp_dbpy_schema(table_name varchar, column_name varchar, data_type varchar);")
        for row in rows_to_insert:
            self.cur.execute("insert into tmp_dbpy_schema(table_name, column_name, data_type) values('%s', '%s', '%s');" % row)
        self.cur.execute("SELECT name, sql  FROM sqlite_master where sql like '%REFERENCES%';")
        # grep for foreign keys

        self.cur.execute("drop table if exists tmp_dbpy_foreign_keys;")
        self.cur.execute("create temp table tmp_dbpy_foreign_keys(table_name varchar, column_name varchar, foreign_table varchar, foreign_column varchar);")
        foreign_keys = []
        self.cur.execute("SELECT name, sql  FROM sqlite_master ;")
        for (table_name, sql) in self.cur:
            rgx = "FOREIGN KEY \(\[(.*)\]\) REFERENCES \[(.*)\] \(\[(.*)\]\)"
            if sql is None:
                continue
            for (column_name, foreign_table, foreign_key) in re.findall(rgx, sql):
                foreign_keys.append((table_name, column_name, foreign_table, foreign_key))
        for row in foreign_keys:
            sql_insert = "insert into tmp_dbpy_foreign_keys(table_name, column_name, foreign_table, foreign_column) values('%s', '%s', '%s', '%s');"
            self.cur.execute(sql_insert % row)

        self.con.commit()
        sys.stderr.write("finished!\n")

    def refresh_schema(self, exclude_system_tables=True):
        """
        Pulls your database's schema again and looks for any new tables and
        columns.
        """

        if exclude_system_tables==True:
            q = self._query_templates['system']['schema_no_system']
        else:
            q = self._query_templates['system']['schema_with_system']

        tables = set()
        self.cur.execute(q)
        cols = []
        tables = {}
        for (table_name, column_name, data_type)in self.cur:
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(Column(self.con, self._query_templates, table_name, column_name, data_type))

        self.tables = TableSet([Table(self.con, self._query_templates, t, tables[t]) for t in sorted(tables.keys())])

    def to_redshift(self, df, table, bucket_name=None, AWS_ACCESS_KEY=None,
                    AWS_SECRET_KEY=None):
        """
        Uploads a data.frame to redshift
        """
        from boto.s3.connection import S3Connection
        from boto.s3.key import Key
        from filechunkio import FileChunkIO

        conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        if bucket_name:
            bucket = conn.create_bucket(bucket_name)
        else:
            bucket = conn.create_bucket("dbpy-" + str(uuid.uuid4()))
        # Get file info
        source_path = 'path/to/your/file.ext'
        source_size = os.stat(source_path).st_size

        # Create a multipart upload request
        mp = bucket.initiate_multipart_upload(os.path.basename(source_path))

        # Use a chunk size of 50 MiB (feel free to change this)
        chunk_size = 52428800
        chunk_count = int(math.ceil(source_size / chunk_size))

        # Send the file parts, using FileChunkIO to create a file-like object
        # that points to a certain byte range within the original file. We
        # set bytes to never exceed the original file size.
        for i in range(chunk_count + 1):
            offset = chunk_size * i
            bytes = min(chunk_size, source_size - offset)
            with FileChunkIO(source_path, 'r', offset=offset,
                                 bytes=bytes) as fp:
                mp.upload_part_from_file(fp, part_num=i + 1)

        # Finish the upload
        mp.complete_upload()

        # TODO: \COPY from <boto> to <database>


def list_profiles(self):
    """
    Lists all of the database profiles available

    Examples
    --------
    >>> from db import list_profiles
    >>> list_profiles()
    """
    profiles = []
    user = os.path.expanduser("~")
    for f in os.listdir(user):
        if f.startswith(".db.py_"):
            profile = os.path.join(user, f)
            profiles.append(base64.decodestring(open(profile).read()))
    return profiles


def DemoDB():
    """
    Provides an instance of DB that hooks up to the Chinook DB
    See http://chinookdatabase.codeplex.com/ for more info.
    """
    _ROOT = os.path.abspath(os.path.dirname(__file__))
    chinook = os.path.join(_ROOT, 'data', "chinook.sqlite")
    return DB(filename=chinook, dbtype="sqlite")
