queries = {
    "column": {
        "head": "select %s from %s limit %d;",
        "all": "select %s from %s;",
        "unique": "select distinct %s from %s;",
        "sample": "select %s from %s order by rand() limit %d;"
    },
    "table": {
        "select": "select %s from %s;",
        "head": "select * from %s limit %d;",
        "all": "select * from %s;",
        "unique": "select distinct %s from %s;",
        "sample": "select * from %s order by rand() limit %d;"
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
            table_name = '%s'
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
            table_name = '%s'
            and column_name = '%s'
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
                referenced_table_name = '%s'
                and referenced_column_name IS NOT NULL;
        """
    }
}
