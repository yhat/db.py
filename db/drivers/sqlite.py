import re
import sys

from db.drivers.dbdriver import DBDriver


class SQLite3Driver(DBDriver):
    def __init__(self, filename):
        try:
            import sqlite3
        except:
            raise Exception("Couldn't find sqlite3 library. Please ensure it is installed")

        con = sqlite3.connect(filename)
        cursor = con.cursor()
        self._create_sqlite_metatable(con, cursor)

        super(SQLite3Driver, self).__init__(con=con, cursor=cursor)

    def _create_sqlite_metatable(self, con, cursor):
        """
        SQLite doesn't come with any metatables (at least ones that fit into our
        framework), so we're going to create them.
        """
        sys.stderr.write("Indexing schema. This will take a second...")
        rows_to_insert = []
        tables = [row[0] for row in cursor.execute("select name from sqlite_master where type='table';")]
        for table in tables:
            for row in cursor.execute("pragma table_info(%s)" % table):
                rows_to_insert.append((table, row[1], row[2]))
        # find for table and column names
        cursor.execute("drop table if exists tmp_dbpy_schema;")
        cursor.execute("create temp table tmp_dbpy_schema(table_name varchar, column_name varchar, data_type varchar);")
        for row in rows_to_insert:
            cursor.execute("insert into tmp_dbpy_schema(table_name, column_name, data_type) values('%s', '%s', '%s');" % row)
        cursor.execute("SELECT name, sql  FROM sqlite_master where sql like '%REFERENCES%';")

        # find for foreign keys
        cursor.execute("drop table if exists tmp_dbpy_foreign_keys;")
        cursor.execute("create temp table tmp_dbpy_foreign_keys(table_name varchar, column_name varchar, foreign_table varchar, foreign_column varchar);")
        foreign_keys = []
        cursor.execute("SELECT name, sql  FROM sqlite_master ;")
        for (table_name, sql) in cursor:
            rgx = "FOREIGN KEY \(\[(.*)\]\) REFERENCES \[(.*)\] \(\[(.*)\]\)"
            if sql is None:
                continue
            for (column_name, foreign_table, foreign_key) in re.findall(rgx, sql):
                foreign_keys.append((table_name, column_name, foreign_table, foreign_key))
        for row in foreign_keys:
            sql_insert = "insert into tmp_dbpy_foreign_keys(table_name, column_name, foreign_table, foreign_column) values('%s', '%s', '%s', '%s');"
            cursor.execute(sql_insert % row)

        con.commit()
        sys.stderr.write("finished!\n")
