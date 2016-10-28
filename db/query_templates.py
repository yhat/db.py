from .queries import mysql as mysql_templates
from .queries import postgres as postgres_templates
from .queries import sqlite as sqlite_templates
from .queries import mssql as mssql_templates

query_templates = {
    "mysql": mysql_templates,
    "postgres": postgres_templates,
    "redshift": postgres_templates,
    "sqlite": sqlite_templates,
    "mssql": mssql_templates,
}
