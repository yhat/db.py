from db.drivers.dbdriver import DBDriver


class MySQLDriver(DBDriver):
    def __init__(self, host, port, user, passwd, db, driver):
        if driver == "mysqldb":
            con, cursor = self._init_mysqldb_driver(
                host=host, port=port, user=user, passwd=passwd, db=db)
        elif driver == "pymysql":
            con, cursor = self._init_pymysql_driver(
                host=host, port=port, user=user, passwd=passwd, db=db)
        else:
            raise Exception("Driver name must be either mysqldb or pymysql.")

        super(MySQLDriver, self).__init__(con=con, cursor=cursor)

    def _init_mysqldb_driver(self, host, port, user, passwd, db):
        try:
            import MySQLdb
        except:
            raise Exception("Couldn't find MySQLdb library. Please ensure it is installed")

        con = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db)
        cursor = con.cursor()

        return con, cursor

    def _init_pymysql_driver(self, host, port, user, passwd, db):
        try:
            import pymysql
        except:
            raise Exception("Couldn't find pymysql library. Please ensure it is installed")

        con = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
        cursor = con.cursor()

        return con, cursor
