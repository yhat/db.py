queries = {
    "dbtype": "sqlite",
    "column": {
        "head": "select {column} from {table} limit {n};",
        "all": "select {column} from {table};",
        "unique": "select distinct {column} from {table};",
        "sample": "select {column} from {table} order by random() limit {n};"
    },
    "table": {
        "select": "select {columns} from {table};",
        "head": "select * from {table} limit {n};",
        "all": "select * from {table};",
        "unique": "select distinct {columns} from {table};",
        "sample": "select * from {table} order by random() limit {n};"
    },
    "system": {
        "schema_no_system": "select 'public', table_name, column_name, data_type from tmp_dbpy_schema;",
        "schema_with_system": "select 'public', table_name, column_name, data_type from tmp_dbpy_schema;",
        "foreign_keys_for_table": """
            select
                column_name
                , 'public' as foreign_table_schema
                , foreign_table as foreign_table_name
                , foreign_column as foreign_column_name
            from
                tmp_dbpy_foreign_keys
            where
                table_name = '{table}';
        """,
        "foreign_keys_for_column": """
            select
                column_name
                , 'public' as ref_table_schema
                , foreign_table as foreign_table_name
                , foreign_column as foreign_column_name
            from
                tmp_dbpy_foreign_keys
            where
                table_name = '{table}' and column_name = '{column}';
        """,
        "ref_keys_for_table": """
            select
                 foreign_column
                 , 'schema' as schema
                 , table_name
                 , column_name
            from
                tmp_dbpy_foreign_keys
            where
                foreign_table = '{table}';
        """,
    }
}
