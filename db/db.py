import threading
import glob
import gzip
try:
    from StringIO import StringIO  # Python 2.7
except:
    from io import StringIO  # Python 3.3+
import uuid
import json
import base64
import re
import os
import sys

import pandas as pd
from prettytable import PrettyTable
import pybars 

from .queries import mysql as mysql_templates
from .queries import postgres as postgres_templates
from .queries import sqlite as sqlite_templates
from .queries import mssql as mssql_templates


queries_templates = {
    "mysql": mysql_templates,
    "postgres": postgres_templates,
    "redshift": postgres_templates,
    "sqlite": sqlite_templates,
    "mssql": mssql_templates,
}

# attempt to import the relevant database libraries
# TODO: maybe add warnings?
try:
    import psycopg2 as pg
    HAS_PG = True
except ImportError:
    HAS_PG = False

try:
    import MySQLdb
    mysql_connect = MySQLdb.connect
    HAS_MYSQL = True
except ImportError:
    try:
        import pymysql
        mysql_connect = pymysql.connect
        HAS_MYSQL = True
    except ImportError:
        HAS_MYSQL = False

try:
    import sqlite3 as sqlite
    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False

try:
    import pyodbc
    HAS_ODBC = True
except ImportError:
    HAS_ODBC = False

try:
    import pymssql
    HAS_PYMSSQL = True
except ImportError:
    HAS_PYMSSQL = False


class Column(object):
    """
    A Columns is an in-memory reference to a column in a particular table. You
    can use it to do some basic DB exploration and you can also use it to
    execute simple queries.
    """
    def __init__(self, con, query_templates, table, name, dtype, keys_per_column):
        self._con = con
        self._query_templates = query_templates
        self.table = table
        self.name = name
        self.type = dtype
        self.keys_per_column = keys_per_column

        self.foreign_keys = []
        self.ref_keys = []

    def __repr__(self):
        tbl = PrettyTable(["Table", "Name", "Type", "Foreign Keys",
                           "Reference Keys"])
        tbl.add_row([self.table, self.name, self.type, self._str_foreign_keys(),
                     self._str_ref_keys()])
        return str(tbl)

    def __str__(self):
        return "Column({0})<{1}>".format(self.name, self.__hash__())

    def _repr_html_(self):
        tbl = PrettyTable(["Table", "Name", "Type"])
        tbl.add_row([self.table, self.name, self.type])
        return tbl.get_html_string()

    def _str_foreign_keys(self):
        keys = []
        for col in self.foreign_keys:
            keys.append("%s.%s" % (col.table, col.name))
        if self.keys_per_column is not None and len(keys) > self.keys_per_column:
            keys = keys[0:self.keys_per_column] + ['(+ {0} more)'.format(len(keys)-self.keys_per_column)]
        return ", ".join(keys)

    def _str_ref_keys(self):
        keys = []
        for col in self.ref_keys:
            keys.append("%s.%s" % (col.table, col.name))
        if self.keys_per_column is not None and len(keys) > self.keys_per_column:
            keys = keys[0:self.keys_per_column] + ['(+ {0} more)'.format(len(keys)-self.keys_per_column)]
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
        q = self._query_templates['column']['head'].format(column=self.name, table=self.table, n=n)
        return pd.io.sql.read_sql(q, self._con)[self.name]

    def all(self):
        """
        Returns entire column  as a DataFrame. This is executing:
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
        q = self._query_templates['column']['all'].format(column=self.name, table=self.table)
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
        q = self._query_templates['column']['unique'].format(column=self.name, table=self.table)
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
        q = self._query_templates['column']['sample'].format(column=self.name, table=self.table, n=n)
        return pd.io.sql.read_sql(q, self._con)[self.name]

class Table(object):
    """
    A Table is an in-memory reference to a table in a database. You can use it to get more info
    about the columns, schema, etc. of a table and you can also use it to execute queries.
    """
    def __init__(self, con, query_templates, name, cols, keys_per_column):
        self.name = name
        self._con = con
        self._cur = con.cursor()
        self._query_templates = query_templates
        self.foreign_keys = []
        self.ref_keys = []
        self.keys_per_column = keys_per_column

        self._columns = cols
        for col in cols:
            attr = col.name
            if attr in ("name", "con"):
                attr = "_" + col.name
            setattr(self, attr, col)

        self._cur.execute(self._query_templates['system']['foreign_keys_for_table'].format(table=self.name))
        for (column_name, foreign_table, foreign_column) in self._cur:
            col = getattr(self, column_name)
            foreign_key = Column(con, queries_templates, foreign_table, foreign_column, col.type, self.keys_per_column)
            self.foreign_keys.append(foreign_key)
            col.foreign_keys.append(foreign_key)
            setattr(self, column_name, col)

        self.foreign_keys = ColumnSet(self.foreign_keys)

        self._cur.execute(self._query_templates['system']['ref_keys_for_table'].format(table=self.name))
        for (column_name, ref_table, ref_column) in self._cur:
            col = getattr(self, column_name)
            ref_key = Column(con, queries_templates, ref_table, ref_column, col.type, self.keys_per_column)
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
        return "Table({0})<{1}>".format(self.name, self.__hash__())

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
        q = self._query_templates['table']['select'].format(columns=", ".join(args), table=self.name)
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
        q = self._query_templates['table']['head'].format(table=self.name, n=n)
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

        q = self._query_templates['table']['all'].format(table=self.name)
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
        q = self._query_templates['table']['unique'].format(columns=columns, table=self.name)
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
        q = self._query_templates['table']['sample'].format(table=self.name, n=n)
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
    Set of Columns. Used for displaying search results in terminal/ipython
    notebook.
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

class S3(object):
    """
    Simple object for storing AWS credentials
    """
    def __init__(self, access_key, secret_key, profile=None):

        if profile:
            self.load_credentials(profile)
        else:
            self.access_key = access_key
            self.secret_key = secret_key

    def save_credentials(self, profile):
        """
        Saves credentials to a dotfile so you can open them grab them later.

        Parameters
        ----------
        profile: str
            name for your profile (i.e. "dev", "prod")
        """
        home = os.path.expanduser("~")
        filename = os.path.join(home, ".db.py_s3_" + profile)
        creds = {
            "access_key": self.access_key,
            "secret_key": self.secret_key
        }
        with open(filename, 'wb') as f:
            data = json.dumps(creds)
            try:
                f.write(base64.encodestring(data))
            except:
                f.write(base64.encodestring(bytes(data, 'utf-8')))

    def load_credentials(self, profile):
        """
        Loads crentials for a given profile. Profiles are stored in
        ~/.db.py_s3_{profile_name} and are a base64 encoded JSON file. This is
        not to say this a secure way to store sensitive data, but it will
        probably stop your little sister from spinning up EC2 instances.

        Parameters
        ----------
        profile: str
            identifier/name for your database (i.e. "dev", "prod")
        """
        user = os.path.expanduser("~")
        f = os.path.join(user, ".db.py_s3_" + profile)
        if os.path.exists(f):
            creds = json.loads(base64.decodestring(open(f, 'rb').read()).encode('utf-8'))
            if 'access_key' not in creds:
                raise Exception("`access_key` not found in s3 profile '{0}'".format(profile))
            self.access_key = creds['access_key']
            if 'access_key' not in creds:
                raise Exception("`secret_key` not found in s3 profile '{0}'".format(profile))
            self.secret_key = creds['secret_key']


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
        Port the database is running on. defaults to default port for db.
            portgres: 5432
            redshift: 5439
            mysql: 3306
            sqlite: n/a
            mssql: 1433
    filename: str
        path to sqlite database
    dbname: str
        Name of the database
    schemas: list
        List of schemas to include. Defaults to all.
    profile: str
        Preconfigured database credentials / profile for how you like your queries
    exclude_system_tables: bool
        Whether or not to include "system" tables (the ones that the database needs
        in order to operate). This includes things like schema definitions. Most of
        you probably don't need this, but if you're a db admin you might actually
        want to query the system tables.
    limit: int, None
        Default number of records to return in a query. This is used by the DB.query
        method. You can override it by adding limit={X} to the `query` method, or
        by passing an argument to `DB()`. None indicates that there will be no
        limit (That's right, you'll be limitless. Bradley Cooper style.)
    keys_per_column: int, None
        Default number of keys to display in the foreign and reference keys.
        This is used to control the rendering of PrettyTable a bit. None means
        that you'll have verrrrrrrry wide columns in some cases.

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
    >>> db = DB(dbname="AdventureWorks2012", dbtype="mssql")
    """
    def __init__(self, username=None, password=None, hostname="localhost",
            port=None, filename=None, dbname=None, dbtype=None, schemas=None,
            profile="default", exclude_system_tables=True, limit=1000, keys_per_column=None):

        if port is None:
            if dbtype=="postgres":
                port = 5432
            elif dbtype=="redshift":
                port = 5439
            elif dbtype=="mysql":
                port = 3306
            elif dbtype=="sqlite":
                port = None
            elif dbtype=="mssql":
                port = 1433
            elif profile is not None:
                pass
            else:
                raise Exception("Database type not specified! Must select one of: postgres, sqlite, mysql, mssql, or redshift")

        if not dbtype in ("sqlite", "mssql") and username is None:
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
            self.schemas = schemas
            self.limit = limit
            self.keys_per_column = keys_per_column

        if self.dbtype is None:
            raise Exception("Database type not specified! Must select one of: postgres, sqlite, mysql, mssql, or redshift")
        self._query_templates = queries_templates.get(self.dbtype).queries

        if self.dbtype=="postgres" or self.dbtype=="redshift":
            if not HAS_PG:
                raise Exception("Couldn't find psycopg2 library. Please ensure it is installed")
            self.con = pg.connect(user=self.username, password=self.password,
                host=self.hostname, port=self.port, dbname=self.dbname)
            self.cur = self.con.cursor()
        elif self.dbtype=="sqlite":
            if not HAS_SQLITE:
                raise Exception("Couldn't find sqlite library. Please ensure it is installed")
            self.con = sqlite.connect(self.filename)
            self.cur = self.con.cursor()
            self._create_sqlite_metatable()
        elif self.dbtype=="mysql":
            if not HAS_MYSQL:
                raise Exception("Couldn't find MySQLdb or pymysql library. Please ensure it is installed")
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
            self.con = mysql_connect(**creds)
            self.con.autocommit(True)
            self.cur = self.con.cursor()
        elif self.dbtype=="mssql":
            if not HAS_ODBC and not HAS_PYMSSQL:
                raise Exception("Couldn't find pyodbc or pymssql libraries. Please ensure one of them is installed")

            if HAS_ODBC:
                base_con = "Driver={0};Server={server};Database={database};".format(
                    "SQL Server",
                    server=self.hostname or "localhost",
                    database=self.dbname or ''
                )
                conn_str = ((self.username and self.password) and "{}{}".format(
                    base_con,
                    "User Id={username};Password={password};".format(
                        username=self.username,
                        password=self.password
                    )
                ) or "{}{}".format(base_con, "Trusted_Connection=Yes;"))

                self.con = pyodbc.connect(conn_str)
                self.cur = self.con.cursor()
            elif HAS_PYMSSQL:
                if hasattr(self, 'port'):
                    hostname = '{0}:{1}'.format(self.hostname, self.port)
                else:
                    hostname = self.hostname

                self.con = pymssql.connect(host=hostname,
                                           user=self.username,
                                           password=self.password,
                                           database=self.dbname)
                self.cur = self.con.cursor()

        self.tables = TableSet([])
        self.refresh_schema(exclude_system_tables)
        self.handlebars = pybars.Compiler()

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
            raw_creds = open(f, 'rb').read()
            raw_creds = base64.decodestring(raw_creds).decode('utf-8')
            creds = json.loads(raw_creds)
            self.username = creds.get('username')
            self.password = creds.get('password')
            self.hostname = creds.get('hostname')
            self.port = creds.get('port')
            self.filename = creds.get('filename')
            self.dbname = creds.get('dbname')
            self.dbtype = creds.get('dbtype')
            self.schemas = creds.get('schemas')
            self.limit = creds.get('limit')
            self.keys_per_column = creds.get('keys_per_column')
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
        dotfile = os.path.join(user, ".db.py_" + profile)
        creds = {
            "username": self.username,
            "password": self.password,
            "hostname": self.hostname,
            "port": self.port,
            "filename": db_filename,
            "dbname": self.dbname,
            "dbtype": self.dbtype,
            "schemas": self.schemas,
            "limit": self.limit,
            "keys_per_column": self.keys_per_column,
        }
        with open(dotfile, 'wb') as f:
            data = json.dumps(creds)
            try:
                f.write(base64.encodestring(data))
            except:
                f.write(base64.encodestring(bytes(data, 'utf-8')))

    def find_table(self, search):
        """
        Aggresively search through your database's schema for a table.

        Parameters
        -----------
        search: str
           glob pattern for what you're looking for

        Examples
        ----------
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> db.find_table("A*")
            +--------+--------------------------+
            | Table  | Columns                  |
            +--------+--------------------------+
            | Album  | AlbumId, Title, ArtistId |
            | Artist | ArtistId, Name           |
            +--------+--------------------------+
        >>> results = db.find_table("tmp*") # returns all tables prefixed w/ tmp
        >>> results = db.find_table("prod_*") # returns all tables prefixed w/ prod_
        >>> results = db.find_table("*Invoice*") # returns all tables containing trans
        >>> results = db.find_table("*") # returns everything
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
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> db.find_column("Name") # returns all columns named "Name"
        +-----------+-------------+---------------+
        | Table     | Column Name | Type          |
        +-----------+-------------+---------------+
        | Artist    |     Name    | NVARCHAR(120) |
        | Genre     |     Name    | NVARCHAR(120) |
        | MediaType |     Name    | NVARCHAR(120) |
        | Playlist  |     Name    | NVARCHAR(120) |
        | Track     |     Name    | NVARCHAR(200) |
        +-----------+-------------+---------------+
        >>> db.find_column("*Id") # returns all columns ending w/ Id
        +---------------+---------------+---------+
        | Table         |  Column Name  | Type    |
        +---------------+---------------+---------+
        | Album         |    AlbumId    | INTEGER |
        | Album         |    ArtistId   | INTEGER |
        | Artist        |    ArtistId   | INTEGER |
        | Customer      |  SupportRepId | INTEGER |
        | Customer      |   CustomerId  | INTEGER |
        | Employee      |   EmployeeId  | INTEGER |
        | Genre         |    GenreId    | INTEGER |
        | Invoice       |   InvoiceId   | INTEGER |
        | Invoice       |   CustomerId  | INTEGER |
        | InvoiceLine   |   InvoiceId   | INTEGER |
        | InvoiceLine   |    TrackId    | INTEGER |
        | InvoiceLine   | InvoiceLineId | INTEGER |
        | MediaType     |  MediaTypeId  | INTEGER |
        | Playlist      |   PlaylistId  | INTEGER |
        | PlaylistTrack |    TrackId    | INTEGER |
        | PlaylistTrack |   PlaylistId  | INTEGER |
        | Track         |  MediaTypeId  | INTEGER |
        | Track         |    TrackId    | INTEGER |
        | Track         |    AlbumId    | INTEGER |
        | Track         |    GenreId    | INTEGER |
        +---------------+---------------+---------+
        >>> db.find_column("*Address*") # returns all columns containing Address
        +----------+----------------+--------------+
        | Table    |  Column Name   | Type         |
        +----------+----------------+--------------+
        | Customer |    Address     | NVARCHAR(70) |
        | Employee |    Address     | NVARCHAR(70) |
        | Invoice  | BillingAddress | NVARCHAR(70) |
        +----------+----------------+--------------+
        >>> db.find_column("*Address*", data_type="NVARCHAR(70)") # returns all columns containing Address that are varchars
        >>> db.find_column("*e*", data_type=["NVARCHAR(70)", "INTEGER"]) # returns all columns have an "e" and are NVARCHAR(70)S or INTEGERS
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
                q = q.rstrip().rstrip(";")
                q = "select * from ({q}) q limit {limit}".format(q=q, limit=limit)
            return q
        # mssql
        else:
            if limit:
                q = "select top {limit} * from ({q}) q".format(limit=limit, q=q)
            return q
    
    def _apply_handlebars(self, q, data, union=True):
        q = unicode(q)
        template = self.handlebars.compile(q)
        if isinstance(data, list):
            query = [template(item) for item in data] 
            query = [str(item) for item in query]
            if union==True:
                query = "\nUNION ALL".join(query)
            else:
                query = "\n".join(query)
        elif isinstance(data, dict):
            query = template(data)
            query = str(query)
        else:
            return q
        return query

    def query(self, q, data=None, union=True, limit=None):
        """
        Query your database with a raw string.

        Parameters
        ----------
        q: str
            Query string to execute
        data: list, dict
            Optional argument for handlebars-queries. Data will be passed to the
            template and rendered using handlebars.
        union: bool
            Whether or not "UNION ALL" handlebars templates. This will return
            any handlebars queries as a single data frame.
        limit: int
            Number of records to return

        Examples
        --------
        >>> from db import DemoDB
        >>> db.query("select * from Track")
           TrackId                                     Name  AlbumId  MediaTypeId  \
        0        1  For Those About To Rock (We Salute You)        1            1
        1        2                        Balls to the Wall        2            2
        2        3                          Fast As a Shark        3            2

           GenreId                                           Composer  Milliseconds  \
        0        1          Angus Young, Malcolm Young, Brian Johnson        343719
        1        1                                               None        342562
        2        1  F. Baltes, S. Kaufman, U. Dirkscneider & W. Ho...        230619

              Bytes  UnitPrice
        0  11170334       0.99
        1   5510424       0.99
        2   3990994       0.99
        ...
        >>> db.query("select * from Track", limit=10)
           TrackId                                     Name  AlbumId  MediaTypeId  \
        0        1  For Those About To Rock (We Salute You)        1            1
        1        2                        Balls to the Wall        2            2
        2        3                          Fast As a Shark        3            2
        3        4                        Restless and Wild        3            2
        4        5                     Princess of the Dawn        3            2
        5        6                    Put The Finger On You        1            1
        6        7                          Let's Get It Up        1            1
        7        8                         Inject The Venom        1            1
        8        9                               Snowballed        1            1
        9       10                               Evil Walks        1            1

           GenreId                                           Composer  Milliseconds  \
        0        1          Angus Young, Malcolm Young, Brian Johnson        343719
        1        1                                               None        342562
        2        1  F. Baltes, S. Kaufman, U. Dirkscneider & W. Ho...        230619
        3        1  F. Baltes, R.A. Smith-Diesel, S. Kaufman, U. D...        252051
        4        1                         Deaffy & R.A. Smith-Diesel        375418
        5        1          Angus Young, Malcolm Young, Brian Johnson        205662
        6        1          Angus Young, Malcolm Young, Brian Johnson        233926
        7        1          Angus Young, Malcolm Young, Brian Johnson        210834
        8        1          Angus Young, Malcolm Young, Brian Johnson        203102
        9        1          Angus Young, Malcolm Young, Brian Johnson        263497

              Bytes  UnitPrice
        0  11170334       0.99
        1   5510424       0.99
        2   3990994       0.99
        3   4331779       0.99
        4   6290521       0.99
        5   6713451       0.99
        6   7636561       0.99
        7   6852860       0.99
        8   6599424       0.99
        9   8611245       0.99
        >>> q = '''
        SELECT
          a.Title
          , t.Name
          , t.UnitPrice
        FROM
          Album a
        INNER JOIN
          Track t
            on a.AlbumId = t.AlbumId;
        '''
        >>> db.query(q, limit=10)
                                           Title  \
        0  For Those About To Rock We Salute You
        1                      Balls to the Wall
        2                      Restless and Wild
        3                      Restless and Wild
        4                      Restless and Wild
        5  For Those About To Rock We Salute You
        6  For Those About To Rock We Salute You
        7  For Those About To Rock We Salute You
        8  For Those About To Rock We Salute You
        9  For Those About To Rock We Salute You

                                              Name  UnitPrice
        0  For Those About To Rock (We Salute You)       0.99
        1                        Balls to the Wall       0.99
        2                          Fast As a Shark       0.99
        3                        Restless and Wild       0.99
        4                     Princess of the Dawn       0.99
        5                    Put The Finger On You       0.99
        6                          Let's Get It Up       0.99
        7                         Inject The Venom       0.99
        8                               Snowballed       0.99
        9                               Evil Walks       0.99
        >>> template = '''
        SELECT
            '{{ name }}' as table_name
            , COUNT(*) as cnt
        FROM
            {{ name }}
        GROUP BY
            table_name
        '''
        >>> data = [
            {"name": "Album"},
            {"name": "Artist"},
            {"name": "Track"}
        ]
        >>> db.query(q, data=data)
          table_name   cnt
        0      Album   347
        1     Artist   275
        2      Track  3503
        >>> q = '''
        SELECT
        {{#cols}}
            {{#if @last}}
                {{ . }}
            {{else}}
                {{ . }} ,
            {{/if}}
        {{/cols}}
        FROM
            Album;
        '''
        >>> data = {"cols": ["AlbumId", "Title", "ArtistId"]}
        >>> db.query(q, data=data, union=False)
           AlbumId                                  Title  ArtistId
        0        1  For Those About To Rock We Salute You         1
        1        2                      Balls to the Wall         2
        2        3                      Restless and Wild         2
        3        4                      Let There Be Rock         1
        4        5                               Big Ones         3

    """
        if data:
            q = self._apply_handlebars(q, data, union)
        if limit==False:
            pass
        else:
            q = self._assign_limit(q, limit)
        return pd.io.sql.read_sql(q, self.con)

    def query_from_file(self, filename, data, union=True, limit=None):
        """
        Query your database from a file.

        Parameters
        ----------
        filename: str
            A SQL script
        data: list, dict
            Optional argument for handlebars-queries. Data will be passed to the
            template and rendered using handlebars.
        union: bool
            Whether or not "UNION ALL" handlebars templates. This will return
            any handlebars queries as a single data frame.
        limit: int
            Number of records to return

        Examples
        --------
        >>> from db import DemoDB
        >>> q = '''
        SELECT
          a.Title
          , t.Name
          , t.UnitPrice
        FROM
          Album a
        INNER JOIN
          Track t
            on a.AlbumId = t.AlbumId;
        '''
        >>> with open("myscript.sql", "w") as f:
        ...    f.write(q)
        ...
        >>> db.query_from_file(q, limit=10)
                                           Title  \
        0  For Those About To Rock We Salute You
        1                      Balls to the Wall
        2                      Restless and Wild
        3                      Restless and Wild
        4                      Restless and Wild
        5  For Those About To Rock We Salute You
        6  For Those About To Rock We Salute You
        7  For Those About To Rock We Salute You
        8  For Those About To Rock We Salute You
        9  For Those About To Rock We Salute You

                                              Name  UnitPrice
        0  For Those About To Rock (We Salute You)       0.99
        1                        Balls to the Wall       0.99
        2                          Fast As a Shark       0.99
        3                        Restless and Wild       0.99
        4                     Princess of the Dawn       0.99
        5                    Put The Finger On You       0.99
        6                          Let's Get It Up       0.99
        7                         Inject The Venom       0.99
        8                               Snowballed       0.99
        9                               Evil Walks       0.99
        """
        q = open(filename).read()
        if data:
            q = self._apply_handlebars(q, data, union)
        return self.query(q, limit)

    def _create_sqlite_metatable(self):
        """
        SQLite doesn't come with any metatables (at least ones that fit into our
        framework), so we're going to create them.
        """
        sys.stderr.write("Indexing schema. This will take a second...")
        rows_to_insert = []
        tables = [row[0] for row in self.cur.execute("select name from sqlite_master where type='table';")]
        for table in tables:
            for row in self.cur.execute("pragma table_info({0})".format(table)):
                rows_to_insert.append((table, row[1], row[2]))
        # find for table and column names
        self.cur.execute("drop table if exists tmp_dbpy_schema;")
        self.cur.execute("create temp table tmp_dbpy_schema(table_name varchar, column_name varchar, data_type varchar);")
        for row in rows_to_insert:
            self.cur.execute("insert into tmp_dbpy_schema(table_name, column_name, data_type) values('{0}', '{1}', '{2}');".format(*row))
        self.cur.execute("SELECT name, sql  FROM sqlite_master where sql like '%REFERENCES%';")

        # find for foreign keys
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
            sql_insert = "insert into tmp_dbpy_foreign_keys(table_name, column_name, foreign_table, foreign_column) values('{0}', '{1}', '{2}', '{3}');"
            self.cur.execute(sql_insert.format(*row))

        self.con.commit()
        sys.stderr.write("finished!\n")

    def refresh_schema(self, exclude_system_tables=True):
        """
        Pulls your database's schema again and looks for any new tables and
        columns.
        """

        sys.stderr.write("Refreshing schema. Please wait...")
        if self.schemas is not None and isinstance(self.schemas, list) and 'schema_specified' in self._query_templates['system']:
            schemas_str = ','.join([repr(schema) for schema in self.schemas])
            q = self._query_templates['system']['schema_specified'] % schemas_str
        elif exclude_system_tables==True:
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
            tables[table_name].append(Column(self.con, self._query_templates, table_name, column_name, data_type, self.keys_per_column))

        self.tables = TableSet([Table(self.con, self._query_templates, t, tables[t], keys_per_column=self.keys_per_column) for t in sorted(tables.keys())])
        sys.stderr.write("done!\n")

    def _try_command(self, cmd):
        try:
            self.cur.execute(cmd)
        except Exception as e:
            print ("Error executing command:")
            print ("\t '{0}'".format(cmd))
            print ("Exception: {0}".format(e))
            self.con.rollback()

    def to_redshift(self, name, df, drop_if_exists=False, chunk_size=10000,
                    AWS_ACCESS_KEY=None, AWS_SECRET_KEY=None, s3=None,
                    print_sql=False, bucket_location=None, s3_bucket=None):
        """
        Upload a dataframe to redshift via s3.

        Parameters
        ----------
        name: str
            name for your shiny new table
        df: DataFrame
            data frame you want to save to the db
        drop_if_exists: bool (False)
            whether you'd like to drop the table if it already exists
        chunk_size: int (10000)
            Number of DataFrame chunks to upload and COPY from S3. Upload speed
            is *much* faster if chunks = multiple-of-slices. Ex: DW1.XL nodes
            have 2 slices per node, so if running 2 nodes you will want
            chunk_size=4, 8, etc
        AWS_ACCESS_KEY: str
            your aws access key. if this is None, the function will try
            and grab AWS_ACCESS_KEY from your environment variables
        AWS_SECRET_KEY: str
            your aws secrety key. if this is None, the function will try
            and grab AWS_SECRET_KEY from your environment variables
        s3: S3
            alternative to using keys, you can use an S3 object
        print_sql: bool (False)
            option for printing sql statement that will be executed
        bucket_location: boto.s3.connection.Location
            a specific AWS location in which to create the temporary transfer s3
            bucket. This should match your redshift cluster's region.

        Examples
        --------
        """
        if self.dbtype!="redshift":
            raise Exception("Sorry, feature only available for redshift.")
        try:
            from boto.s3.connection import S3Connection
            from boto.s3.key import Key
            from boto.s3.connection import Location

            # if boto is present, set the bucket_location to default.
            # we can't do this in the function definition because we're 
            # lazily importing boto only if necessary here.
            if bucket_location is None:
                bucket_location = Location.DEFAULT

        except ImportError:
            raise Exception("Couldn't find boto library. Please ensure it is installed")

        if s3 is not None:
            AWS_ACCESS_KEY = s3.access_key
            AWS_SECRET_KEY = s3.secret_key
        if AWS_ACCESS_KEY is None:
            AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
        if AWS_SECRET_KEY is None:
            AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
        if AWS_ACCESS_KEY is None:
            raise Exception("Must specify AWS_ACCESS_KEY as either function argument or as an environment variable `AWS_ACCESS_KEY`")
        if AWS_SECRET_KEY is None:
            raise Exception("Must specify AWS_SECRET_KEY as either function argument or as an environment variable `AWS_SECRET_KEY`")

        conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        #this way users with permission on specific buckets can use this feature
        bucket_name = "dbpy-{0}".format(uuid.uuid4())
        if s3_bucket:
            bucket = conn.get_bucket(s3_bucket)
            bucket_name = s3_bucket
        else:
            bucket = conn.create_bucket(bucket_name, location=bucket_location)
        # we're going to chunk the file into pieces. according to amazon, this is
        # much faster when it comes time to run the \COPY statment.
        #
        # see http://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html
        sys.stderr.write("Transfering {0} to s3 in chunks".format(name))
        len_df = len(df)
        chunks = range(0, len_df, chunk_size)
        def upload_chunk(i):
            conn = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
            chunk = df[i:(i+chunk_size)]
            k = Key(bucket)
            k.key = 'data-%d-%d.csv.gz' % (i, i + chunk_size)
            k.set_metadata('parent', 'db.py')
            out = StringIO()
            with gzip.GzipFile(fileobj=out, mode="w") as f:
                  f.write(chunk.to_csv(index=False, encoding='utf-8'))
            k.set_contents_from_string(out.getvalue())
            sys.stderr.write(".")
            return i

        threads = []
        for i in chunks:
            t = threading.Thread(target=upload_chunk, args=(i, ))
            t.start()
            threads.append(t)

        # join all threads
        for t in threads:
            t.join()
        sys.stderr.write("done\n")

        if drop_if_exists:
            sql = "DROP TABLE IF EXISTS {0};".format(name)
            if print_sql:
                sys.stderr.write(sql + "\n")
            self._try_command(sql)

        # generate schema from pandas and then adapt for redshift
        sql = pd.io.sql.get_schema(df, name)
        # defaults to using SQLite format. need to convert it to Postgres
        sql = sql.replace("[", "").replace("]", "")
        # we'll create the table ONLY if it doens't exist
        sql = sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        if print_sql:
            sys.stderr.write(sql + "\n")
        self._try_command(sql)
        self.con.commit()

        # perform the \COPY here. the s3 argument is a prefix, so it'll pick up
        # all of the data*.gz files we've created
        sys.stderr.write("Copying data from s3 to redshfit...")
        sql = """
        copy {name} from 's3://{bucket_name}/data'
        credentials 'aws_access_key_id={AWS_ACCESS_KEY};aws_secret_access_key={AWS_SECRET_KEY}'
        CSV IGNOREHEADER as 1 GZIP;
        """.format(name=name, bucket_name=bucket_name,
                   AWS_ACCESS_KEY=AWS_ACCESS_KEY, AWS_SECRET_KEY=AWS_SECRET_KEY)
        if print_sql:
            sys.stderr.write(sql + "\n")
        self._try_command(sql)
        self.con.commit()
        sys.stderr.write("done!\n")
        # tear down the bucket
        sys.stderr.write("Tearing down bucket...")
        for key in bucket.list():
            key.delete()
        if not s3_bucket:
            conn.delete_bucket(bucket_name)
        sys.stderr.write("done!")

def list_profiles():
    """
    Lists all of the database profiles available

    Examples
    --------
    >>> from db import list_profiles
    >>> list_profiles()
    {'demo': {u'dbname': None,
      u'dbtype': u'sqlite',
      u'filename': u'/Users/glamp/repos/yhat/opensource/db.py/db/data/chinook.sqlite',
      u'hostname': u'localhost',
      u'password': None,
      u'port': 5432,
      u'username': None},
     'muppets': {u'dbname': u'muppetdb',
      u'dbtype': u'postgres',
      u'filename': None,
      u'hostname': u'muppets.yhathq.com',
      u'password': None,
      u'port': 5432,
      u'username': u'kermit'}}
    """
    profiles = {}
    user = os.path.expanduser("~")
    for f in os.listdir(user):
        if f.startswith(".db.py_"):
            profile = os.path.join(user, f)
            profile = json.loads(base64.decodestring(open(profile).read()))
            profiles[f[7:]] = profile
    return profiles


def remove_profile(name, s3=False):
    """
    Removes a profile from your config
    """
    user = os.path.expanduser("~")
    if s3==True:
        f = os.path.join(user, ".db.py_s3_" + name)
    else:
        f = os.path.join(user, ".db.py_" + name)
    try:
        try:
            open(f)
        except:
            raise Exception("Profile '{0}' does not exist. Could not find file {1}".format(name, f))
        os.remove(f)
    except Exception as e:
        raise Exception("Could not remove profile {0}! Excpetion: {1}".format(name, e))


def DemoDB(keys_per_column=None):
    """
    Provides an instance of DB that hooks up to the Chinook DB
    See http://chinookdatabase.codeplex.com/ for more info.
    """
    _ROOT = os.path.abspath(os.path.dirname(__file__))
    chinook = os.path.join(_ROOT, 'data', "chinook.sqlite")
    return DB(filename=chinook, dbtype="sqlite", keys_per_column=keys_per_column)
