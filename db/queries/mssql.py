queries = {
    "column": {
        "head": "select top %s %s from %s;",
        "all": "select %s from %s;",
        "unique": "select distinct %s from %s;",
        "sample": "select top %s %s from %s order by rand();"
    },
    "table": {
        "select": "select %s from %s;",
        "head": "select top %s * from %s;",
        "all": "select * from %s;",
        "unique": "select distinct %s from %s;",
        "sample": "select top %s * from %s order by rand();"
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
                    table_schema not in ('information_schema', 'sys')
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
                    , data_type
                from
                    information_schema.columns
                where table_schema in (%s);
                """,
        "foreign_keys_for_table": """
            SELECT c1.name AS foreign_key,
                o2.name AS referenced_table,
                c2.name AS referenced_column
            FROM sys.objects o1
                INNER JOIN sys.foreign_keys fk
                    ON o1.object_id = fk.parent_object_id
                INNER JOIN sys.foreign_key_columns fkc
                    ON fk.object_id = fkc.constraint_object_id
                INNER JOIN sys.columns c1
                    ON fkc.parent_object_id = c1.object_id
                    AND fkc.parent_column_id = c1.column_id
                INNER JOIN sys.columns c2
                    ON fkc.referenced_object_id = c2.object_id
                    AND fkc.referenced_column_id = c2.column_id
                INNER JOIN sys.objects o2
                    ON fk.referenced_object_id = o2.object_id
                INNER JOIN sys.key_constraints pk
                    ON fk.referenced_object_id = pk.parent_object_id
                    AND fk.key_index_id = pk.unique_index_id
            WHERE fkc.parent_object_id = object_id('%s');
        """,
        "foreign_key_name_for_table": """
            SELECT
                object_name(constraint_object_id) AS foreign_key,
                object_name(referenced_object_id) AS referenced_table,
                col.name AS referenced_column
            FROM sys.foreign_key_columns
            INNER JOIN sys.columns col
                ON col.column_id = referenced_column_id
                    AND col.object_id = referenced_object_id
            WHERE parent_object_id = object_id('%s');
        """,
        "foreign_keys_for_column": """
            SELECT c1.name AS foreign_key,
                o2.name AS referenced_table,
                c2.name AS referenced_column
            FROM sys.objects o1
                INNER JOIN sys.foreign_keys fk
                    ON o1.object_id = fk.parent_object_id
                INNER JOIN sys.foreign_key_columns fkc
                    ON fk.object_id = fkc.constraint_object_id
                INNER JOIN sys.columns c1
                    ON fkc.parent_object_id = c1.object_id
                    AND fkc.parent_column_id = c1.column_id
                INNER JOIN sys.columns c2
                    ON fkc.referenced_object_id = c2.object_id
                    AND fkc.referenced_column_id = c2.column_id
                INNER JOIN sys.objects o2
                    ON fk.referenced_object_id = o2.object_id
                INNER JOIN sys.key_constraints pk
                    ON fk.referenced_object_id = pk.parent_object_id
                    AND fk.key_index_id = pk.unique_index_id
            WHERE fkc.constraint_object_id = object_id('%s');
        """,
        "foreign_key_name_for_column": """
            SELECT
                object_name(constraint_object_id) AS foreign_key,
                object_name(referenced_object_id) AS referenced_table,
                col.name AS referenced_column
            FROM sys.foreign_key_columns
            INNER JOIN sys.columns col
                ON col.column_id = referenced_column_id
                    AND col.object_id = referenced_object_id
            WHERE parent_object_id = object_id('%s')
                AND constraint_object_id = object_id('%s');
        """,
        "ref_keys_for_table": """
            SELECT
                c.Name AS constraint_column,
                t.Name AS referenced_table,
                c.Name AS referenced_column
            FROM sys.tables t
            INNER JOIN sys.default_constraints dc
                ON t.object_id = dc.parent_object_id
            INNER JOIN sys.columns c
                ON dc.parent_object_id = c.object_id
                    AND c.column_id = dc.parent_column_id
            WHERE t.name='%s';
        """
    }
}
