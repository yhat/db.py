from prettytable import PrettyTable
import pandas as pd


class Column(object):
    """
    A Columns is an in-memory reference to a column in a particular table. You
    can use it to do some basic DB exploration and you can also use it to
    execute simple queries.
    """

    def __init__(self, con, query_templates, schema, table, name, dtype, keys_per_column):
        self._con = con
        self._query_templates = query_templates
        self.schema = schema
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
            keys = keys[0:self.keys_per_column] + ['(+ {0} more)'.format(len(keys) - self.keys_per_column)]
        return ", ".join(keys)

    def _str_ref_keys(self):
        keys = []
        for col in self.ref_keys:
            keys.append("%s.%s" % (col.table, col.name))
        if self.keys_per_column is not None and len(keys) > self.keys_per_column:
            keys = keys[0:self.keys_per_column] + ['(+ {0} more)'.format(len(keys) - self.keys_per_column)]
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
        q = self._query_templates['column']['head'].format(column=self.name, schema=self.schema,
                                                           table=self.table, n=n)
        return pd.read_sql(q, self._con)[self.name]

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
        >>> db.tables.Customer.Email.all().head()
        0        luisg@embraer.com.br
        1       leonekohler@surfeu.de
        2         ftremblay@gmail.com
        3       bjorn.hansen@yahoo.no
        4    frantisekw@jetbrains.com
        Name: Email, dtype: object
        >>> df = db.tables.Customer.Email.all()
        >>> len(df)
        59
        """
        q = self._query_templates['column']['all'].format(column=self.name, schema=self.schema,
                                                          table=self.table)
        return pd.read_sql(q, self._con)[self.name]

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
        >>> db.tables.Customer.FirstName.unique().head(10)
        0         Luis
        1       Leonie
        2     Francois
        3        Bjorn
        4    Franti\u0161ek
        5       Helena
        6       Astrid
        7         Daan
        8         Kara
        9      Eduardo
        Name: FirstName, dtype: object
        >>> len(db.tables.Customer.LastName.unique())
        59
        """
        q = self._query_templates['column']['unique'].format(column=self.name, schema=self.schema,
                                                             table=self.table)
        return pd.read_sql(q, self._con)[self.name]

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

        Examples (removed from doctest as we can't predict random names...)
        --------
        from db import DemoDB
        db = DemoDB()
        db.tables.Artist.Name.sample(10)
        0                        Pedro Luis & A Parede
        1                   Santana Feat. Eric Clapton
        2                                  Os Mutantes
        3                              Banda Black Rio
        4               Adrian Leaper & Doreen de Feis
        5    Chicago Symphony Orchestra & Fritz Reiner
        6                            Smashing Pumpkins
        7                                   Spyro Gyra
        8    Aaron Copland & London Symphony Orchestra
        9      Sir Georg Solti & Wiener Philharmoniker
        Name: Name, dtype: object
        >>> from db import DemoDB
        >>> db = DemoDB()
        >>> df = db.tables.Artist.Name.sample(10)
        >>> len(df)
        10
        """
        q = self._query_templates['column']['sample'].format(column=self.name, schema=self.schema,
                                                             table=self.table, n=n)
        return pd.read_sql(q, self._con)[self.name]

    def to_dict(self):
        """
        Serialize representation of the column for local caching.
        """
        return {'schema': self.schema, 'table': self.table, 'name': self.name, 'type': self.type}

class ColumnSet(object):
    """
    Set of Columns. Used for displaying search results in terminal/ipython
    notebook.
    """

    def __init__(self, columns):
        self.columns = columns
        self.pretty_tbl_cols = ["Table", "Column Name", "Type"]
        self.use_schema = False

        for col in columns:
            if col.schema and not self.use_schema:
                self.use_schema = True
                self.pretty_tbl_cols.insert(0, "Schema")

    def __getitem__(self, i):
        return self.columns[i]

    def _tablify(self):
        tbl = PrettyTable(self.pretty_tbl_cols)

        for col in self.pretty_tbl_cols:
            tbl.align[col] = "l"

        for col in self.columns:
            row_data = [col.table, col.name, col.type]
            if self.use_schema:
                row_data.insert(0, col.schema)
            tbl.add_row(row_data)
        return tbl

    def __repr__(self):
        tbl = str(self._tablify())
        return tbl

    def _repr_html_(self):
        return self._tablify().get_html_string()

    def to_dict(self):
        """Serialize representation of the tableset for local caching."""
        return {'columns': [col.to_dict() for col in self.columns]}
