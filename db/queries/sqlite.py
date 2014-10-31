queries = {
    "column": {
        "head": "select %s from %s limit %d;",
        "all": "select %s from %s;",
        "unique": "select distinct %s from %s;",
        "sample": "select %s from %s order by random() limit %d;"
    },
    "table": {
        "select": "select %s from %s;",
        "head": "select * from %s limit %d;",
        "all": "select * from %s;",
        "unique": "select distinct %s from %s;",
        "sample": "select * from %s order by random() limit %d;"
    },
    "system": {
        "schema_no_system": "select table_name, column_name, data_type from tmp_dbpy_schema;",
        "schema_with_system": "select table_name, column_name, data_type from tmp_dbpy_schema;",
        "foreign_keys_for_table": """
            select
                column_name
                , foreign_table as foreign_table_name
                , foreign_column as foreign_colum_name
            from
                tmp_dbpy_foreign_keys
            where
                table_name = '%s';
        """,
        "foreign_keys_for_column": """
            select
                column_name
                , foreign_table as foreign_table_name
                , foreign_column as foreign_colum_name
            from
                tmp_dbpy_foreign_keys
            where
                table_name = '%s' and column_name = '%s';
        """,
        "ref_keys_for_table": """
            select
                 foreign_column
                 , table_name
                 , column_name
            from
                tmp_dbpy_foreign_keys
            where
                foreign_table = '%s';
        """,
    }
}
