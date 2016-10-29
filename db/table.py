from prettytable import PrettyTable
import pandas as pd
from .column import Column, ColumnSet
from .query_templates import query_templates

class Table(object):
    """
    A Table is an in-memory reference to a table in a database. You can use it to get more info
    about the columns, schema, etc. of a table and you can also use it to execute queries.
    """

    def __init__(self, con, query_templates, schema, name, cols, keys_per_column, foreign_keys=None, ref_keys=None):
        self.schema = schema
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
            if attr in ("name", "con", "count"):
                attr = self.name + "_" + col.name
            setattr(self, attr, col)

        # ToDo: factor out common logic below
        # load foreign keys if not provided
        if not isinstance(foreign_keys, list):
            self._cur.execute(self._query_templates['system']['foreign_keys_for_table'].format(table=self.name,
                                                                                               table_schema=self.schema))
            foreign_keys = self._cur

        # build columns from the foreign keys metadata we have
        for (column_name, foreign_table_schema, foreign_table, foreign_column) in foreign_keys:
            col = getattr(self, column_name)
            foreign_key = Column(con, query_templates, foreign_table_schema, foreign_table, foreign_column, col.type, self.keys_per_column)
            self.foreign_keys.append(foreign_key)
            col.foreign_keys.append(foreign_key)
            setattr(self, column_name, col)

        # store the foreign keys as a special group of columns
        self.foreign_keys = ColumnSet(self.foreign_keys)

        # load ref keys if not provided
        if not isinstance(ref_keys, list):
            self._cur.execute(self._query_templates['system']['ref_keys_for_table'].format(table=self.name,
                                                                                           table_schema=self.schema))
            ref_keys = self._cur

        # build columns for the ref key metadata we have
        for (column_name, ref_schema, ref_table, ref_column) in ref_keys:
            col = getattr(self, column_name)
            ref_key = Column(con, query_templates, ref_schema, ref_table, ref_column, col.type, self.keys_per_column)
            self.ref_keys.append(ref_key)
            col.ref_keys.append(ref_key)
            setattr(self, column_name, col)

        # store ref keys as a special group of columns
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
        brk = "+" + "-" * (len(r) - 2) + "+"
        title = "|" + self.name.center(len(r) - 2) + "|"
        return brk + "\n" + title + "\n" + tbl

    def __str__(self):
        return "Table({0})<{1}>".format(self.name, self.__hash__())

    def _repr_html_(self):
        return self._tablify().get_html_string()

    def _format_columns(self, columns):
        if len(columns) == 0:
            return "*"

        if self._query_templates['dbtype']=="postgres":
            columns = ['"%s"' % column for column in columns]

        return ", ".join(columns)

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
        >>> db.tables.Track.select("Name")[:1].Name
        0    For Those About To Rock (We Salute You)
        Name: Name, dtype: object

        # select name from the Track table
        db.tables.Track.select("Name")
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
        q = self._query_templates['table']['select'].format(columns=self._format_columns(args), schema=self.schema,
                                                            table=self.name)
        return pd.read_sql(q, self._con)

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


        >>> db.tables.Track.count
        3503

        -= Not in doctest as output is hard to predict
        # select name from the Track table
        db.tables.Track.head()
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

        db.tables.Track.head(1)
           TrackId                                     Name  AlbumId  MediaTypeId  \
        0        1  For Those About To Rock (We Salute You)        1            1

           GenreId                                   Composer  Milliseconds     Bytes  \
        0        1  Angus Young, Malcolm Young, Brian Johnson        343719  11170334

           UnitPrice
        0       0.99
        """
        q = self._query_templates['table']['head'].format(schema=self.schema,
                                                          table=self.name, n=n)
        return pd.read_sql(q, self._con)

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

        q = self._query_templates['table']['all'].format(schema=self.schema,
                                                         table=self.name)
        return pd.read_sql(q, self._con)

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
        q = self._query_templates['table']['unique'].format(columns=self._format_columns(args), schema=self.schema,
                                                            table=self.name)

        return pd.read_sql(q, self._con)

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
        from db import DemoDB
        db = DemoDB()
        Not in doctest : can't predict sample
        db.tables.Track.sample(10)
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
        q = self._query_templates['table']['sample'].format(schema=self.schema,
                                                            table=self.name, n=n)
        return pd.read_sql(q, self._con)

    @property
    def count(self):
        """Return total of rows from table."""
        return len(self.all())

    def to_dict(self):
        """Serialize representation of the table for local caching."""
        return {'schema': self.schema, 'name': self.name, 'columns': [col.to_dict() for col in self._columns],
                'foreign_keys': self.foreign_keys.to_dict(), 'ref_keys': self.ref_keys.to_dict()}

class TableSet(object):
    """
    Set of Tables. Used for displaying search results in terminal/ipython notebook.
    """

    def __init__(self, tables):
        self.pretty_tbl_cols = ["Table", "Columns"]
        self.use_schema = False

        for tbl in tables:
            setattr(self, tbl.name, tbl)
            if tbl.schema and not self.use_schema:
                self.use_schema = True
                self.pretty_tbl_cols.insert(0, "Schema")

        self.tables = tables

    def __getitem__(self, i):
        return self.tables[i]

    def _tablify(self):
        tbl = PrettyTable(self.pretty_tbl_cols)

        for col in self.pretty_tbl_cols:
            tbl.align[col] = "l"

        for table in self.tables:
            column_names = [col.name for col in table._columns]
            column_names = ", ".join(column_names)
            pretty_column_names = ""

            for i in range(0, len(column_names), 80):
                pretty_column_names += column_names[i:(i + 80)] + "\n"
            pretty_column_names = pretty_column_names.strip()
            row_data = [table.name, pretty_column_names]
            if self.use_schema:
                row_data.insert(0, table.schema)
            tbl.add_row(row_data)

        return tbl

    def __repr__(self):
        tbl = str(self._tablify())
        return tbl

    def _repr_html_(self):
        return self._tablify().get_html_string()

    def __len__(self):
        return len(self.tables)

    def to_dict(self):
        """Serialize representation of the tableset for local caching."""
        return {'tables': [table.to_dict() for table in self.tables]}
