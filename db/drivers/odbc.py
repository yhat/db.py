from db.drivers.dbdriver import DBDriver


class ODBCDriver(DBDriver):
    def __init__(self, server, database, username, password):
        try:
            import pyodbc
        except:
            raise Exception("Couldn't find pyodbc library. Please ensure it is installed")

        base_con = "Driver={0};Server={server};Database={database};".format(
            "SQL Server", server=server or "localhost", database=database or ''
        )
        conn_str = ((self.username and self.password) and "{}{}".format(
            base_con,
            "User Id={username};Password={password};".format(
                username=username, password=password
            )
        ) or "{}{}".format(base_con, "Trusted_Connection=Yes;"))

        con = pyodbc.connect(conn_str)
        cursor = con.cursor()

        super(ODBCDriver, self).__init__(con=con, cursor=cursor)
