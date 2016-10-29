queries = {
    "dbtype": "mysql",
    "column": {
        "head": "select {column} from {schema}.{table} limit {n};",
        "all": "select {column} from {schema}.{table};",
        "unique": "select distinct {column} from {schema}.{table};",
        "sample": "select {column} from {schema}.{table} order by rand() limit {n};"
    },
    "table": {
        "select": "select {columns} from {schema}.{table};",
        "head": "select * from {schema}.{table} limit {n};",
        "all": "select * from {schema}.{table};",
        "unique": "select distinct {columns} from {schema}.{table};",
        "sample": "select * from {schema}.{table} order by rand() limit {n};"
    },
    "system": {
        "schema_no_system": """
                select table_schema
                    , table_name
                    , column_name
                    , data_type
                from
                    information_schema.columns
                where
                    table_schema not in ('information_schema', 'performance_schema', 'mysql')
                """,
        "schema_with_system": """
                select table_schema
                    , table_name
                    , column_name
                    , data_type
                from
                    information_schema.columns;
                """,
        "schema_specified": """
                select table_schema
                    , table_name
                    , column_name
                    , udt_name
                from
                    information_schema.columns
                where table_schema in (%s);
                """,
        "foreign_keys_for_table": """
        select column_name
            , referenced_table_schema
            , referenced_table_name
            , referenced_column_name
        from
            information_schema.key_column_usage
        where
            table_name = '{table}'
            and referenced_column_name IS NOT NULL
            and table_schema = '{table_schema}';
        """,
        "foreign_keys_for_column": """
        select column_name
            , referenced_table_schema
            , referenced_table_name
            , referenced_column_name
        from
            information_schema.key_column_usage
        where
            table_name = '{table}'
            and column_name = '{column}'
            and referenced_column_name IS NOT NULL
            and table_schema = '{table_schema}';
        """,
        "ref_keys_for_table": """
            select referenced_column_name
                , table_schema
                , table_name
                , column_name
            from
                information_schema.key_column_usage
            where
                referenced_table_name = '{table}'
                and referenced_column_name IS NOT NULL
                and table_schema = '{table_schema}';
        """,
        "foreign_keys_for_db": """
            select column_name
                , referenced_table_schema
                , referenced_table_name
                , referenced_column_name
            FROM
                information_schema.key_column_usage
            WHERE referenced_column_name IS NOT NULL;
        """,
        "ref_keys_for_db": """
            SELECT referenced_column_name,
                   table_schema,
                   table_name,
                   column_name
            FROM
                information_schema.key_column_usage
            WHERE referenced_column_name IS NOT NULL;
        """
    }
}
