from db.drivers.dbdriver import DBDriver


class PostgreSQLDriver(DBDriver):
    def __init__(self, host, port, user, passwd, db):
        try:
            import psycopg2 as pg
        except:
            raise Exception("Couldn't find psycopg2 library. Please ensure it is installed")

        con = pg.connect(host=host, port=port, user=user, password=passwd, dbname=db)
        cursor = con.cursor()

        super(PostgreSQLDriver, self).__init__(con=con, cursor=cursor)
