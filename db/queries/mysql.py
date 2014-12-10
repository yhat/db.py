queries = {
    "column": {
        "head": "select {column} from {table} limit {n};",
        "all": "select {column} from {table};",
        "unique": "select distinct {column} from {table};",
        "sample": "select {column} from {table} order by rand() limit {n};"
    },
    "table": {
        "select": "select {columns} from {table};",
        "head": "select * from {table} limit {n};",
        "all": "select * from {table};",
        "unique": "select distinct {columns} from {table};",
        "sample": "select * from {table} order by rand() limit {n};"
    },
    "system": {
        "schema_no_system": """
                select
                    table_name
                    , column_name
                    , data_type
                from
                    information_schema.columns
                where
                    table_schema not in ('information_schema', 'performance_schema', 'mysql')
                """,
        "schema_with_system": """
                select
                    table_name
                    , column_name
                    , data_type
                from
                    information_schema.columns;
                """,
        "schema_specified": """
                select
                    table_name
                    , column_name
                    , udt_name
                from
                    information_schema.columns
                where table_schema in (%s);
                """,
        "foreign_keys_for_table": """
        select
            column_name
            , referenced_table_name
            , referenced_column_name
        from
            information_schema.key_column_usage
        where
            table_name = '{table}'
            and referenced_column_name IS NOT NULL;
        """,
        "foreign_keys_for_column": """
        select
            column_name
            , referenced_table_name
            , referenced_column_name
        from
            information_schema.key_column_usage
        where
            table_name = '{table}'
            and column_name = '{column}'
            and referenced_column_name IS NOT NULL;
        """,
        "ref_keys_for_table": """
            select
                referenced_column_name
                , table_name
                , column_name
            from
                information_schema.key_column_usage
            where
                referenced_table_name = '{table}'
                and referenced_column_name IS NOT NULL;
        """
    }
}
