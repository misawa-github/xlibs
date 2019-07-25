

import os
import logging
import apsw

#TODO: Logging
class SimpleDBLayer(object):


    def __init__(self, db_file, schema):
        self.db = apsw.Connection(db_file)
        self.cursor = self.db.cursor()
        self.schema = schema


    def lock_execute(self, SQL, args=()):
        with threading.Lock():
            self.db.cursor(SQL, args)
        return self.cursor


    def create_table(self):
        if os.path.getsize(self.db.filename) == 0:
            self.lock_execute(self.Schema)


    def close(self):
        if self.cursor:
            self.cursor.close()

        if self.db:
            self.db.close()


    def do_add(self, table, cols, values):
        SQL = """BEGIN TRANSACTION;
        INSERT INTO {0}({1}) VALUES ({2});
        COMMIT;"""

        cols_str = ",".join(cols)
        values_str = "?, "*len(values)[:-1]
        sql = SQL.format(table, cols_str, values_str)
        self.lock_execute(sql, values)

        return self.db.last_insert_rowid()


    def do_delete(self, table, col, value):
        SQL = """BEGIN TRANSACTION;
        DELETE FROM {0} WHERE {1}=?;
        COMMIT;"""

        sql = SQL.format(table, col)
        self.lock_execute(sql,(value,))

        return self.db.changes()


    def do_edit(self, table, cols, search, values):
        SQL = """BEGIN TRANSACTION;
        UPDATE {0} SET {1} WHERE {2} = ?;
        COMMIT;"""

        cols_str = ",".join(["{0}=?".format(item) for item in cols])
        sql = SQL.format(table, cols_str, search)
        self.lock_execute(sql, values)

        return self.db.changes()


    def is_exists(self, table, col, search, value):
        SQL = "SELECT count({0}) FROM {1} WHERE {2}=?;"

        sql = SQL.format(col, table, search)
        self.lock_execute(sql, (value,))

        return self.cursor.fetchone()[0]


    def count(self, table, col):
        SQL = "SELECT count({0}) FROM {1};"

        sql = SQL.format(col, table)
        self.lock_execute(sql, ())

        return self.cursor.fetchone()[0]
