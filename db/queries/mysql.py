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
                    table_schema != 'information_schema'
                """,
        "schema_with_system": """
                select
                    table_name
                    , column_name
                    , data_type
                from
                    information_schema.columns;
                """
    }
}