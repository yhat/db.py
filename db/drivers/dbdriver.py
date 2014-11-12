class DBDriver(object):
    def __init__(self, con, cursor):
        self._con = con
        self._cursor = cursor

    @property
    def con(self):
        return self._con

    @property
    def cursor(self):
        return self._cursor
