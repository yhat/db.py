queries = {
    "column": {
        "head": "select %s from %s where rownum<=%d",
        "all": "select %s from %s",
        "unique": "select distinct %s from %s",
        # there is no RANDOM() function in pure SQL, so we'll make it this way
        "sample": "select %s FROM SELECT * FROM (SELECT SYS.DBMS_RANDOM.RANDOM rnd, t.*  FROM %s t1) t2 WHERE rownum<=%d ORDER BY rnd"
    },
    "table": {
        "select": "select %s from %s",
        "head": "select * from %s where rownum<=%d",
        "all": "select * from %s",
        "unique": "select distinct %s from %s",
        "sample": "select %s FROM (SELECT SYS.DBMS_RANDOM.RANDOM rnd, t.*  FROM %s t1) t2 ORDER BY rnd"
    },
    "system": {
        "schema_no_system": """
                select 
                    table_name
                    , column_name
                    , data_type
                FROM USER_TAB_COLUMNS
                """,
        "schema_with_system": """
                select 
                    table_name
                    , column_name
                    , data_type
                FROM ALL_TAB_COLUMNS
        """,
        "schema_specified": """
                select
                    table_name
                    , column_name
                    , data_type
                from USER_TAB_COLUMNS
        """,
        "foreign_keys_for_table": """
                SELECT 
                    a.column_name column_name, 
                    d.table_name referenced_table_name, 
                    d.column_name referenced_column_name
                      FROM all_cons_columns a
                      INNER JOIN all_constraints c ON a.owner = c.owner
                                            AND a.constraint_name = c.constraint_name
                      INNER JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                                               AND c.r_constraint_name = c_pk.constraint_name
                      INNER JOIN all_cons_columns d ON c_pk.owner = d.owner
                                            AND c_pk.constraint_name = d.constraint_name
                    WHERE c.constraint_type = 'R' 
                    AND a.table_name = '%s'
        """,
        "foreign_keys_for_column": """
                SELECT 
                    a.column_name column_name, -- a.constraint_name, c.owner, 
                    d.table_name referenced_table_name, 
                    d.column_name referenced_column_name
                      FROM all_cons_columns a
                      INNER JOIN all_constraints c ON a.owner = c.owner
                                            AND a.constraint_name = c.constraint_name
                      INNER JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                                               AND c.r_constraint_name = c_pk.constraint_name
                      INNER JOIN all_cons_columns d ON c_pk.owner = d.owner
                                            AND c_pk.constraint_name = d.constraint_name
                    WHERE c.constraint_type = 'R' 
                    AND a.table_name = '%s' and a.column_name = '%s'
        """,
        "ref_keys_for_table": """
                SELECT 
                    d.column_name referenced_column_name,
                    a.table_name,
                    a.column_name
                      FROM all_cons_columns a
                      INNER JOIN all_constraints c ON a.owner = c.owner
                                            AND a.constraint_name = c.constraint_name
                      INNER JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                                               AND c.r_constraint_name = c_pk.constraint_name
                      INNER JOIN all_cons_columns d ON c_pk.owner = d.owner
                                            AND c_pk.constraint_name = d.constraint_name
                    WHERE c.constraint_type = 'R' 
                    AND d.table_name = '%s'
        """
    }
}
