import sqlite3
import os

class JobTracker(object):
    def __init__(self):
        self.sqlite = sqlite3.connect(os.path.expanduser('~/influx_sql_connector/jobTracker.db'))
        create_table = ('CREATE TABLE IF NOT EXISTS measurements ('
                       '    id INTEGER PRIMARY KEY AUTOINCREMENT,'
                       '    measure_name TEXT UNIQUE,'
                       '    last_timestamp INTEGER)')
        self.sqlite.execute(create_table)
        self.sqlite.commit()

    def get_last_measure_time(self, measure):
        cursor = self.sqlite.cursor()
        measure_tuple = (measure,)
        cursor.execute('SELECT last_timestamp FROM measurements WHERE measure_name = ?', measure_tuple)
        res = cursor.fetchone()
        if res is None:
            return 0
        else:
            return int(res)

    def insert_measure(self, measure, last_timestamp):
        cursor = self.sqlite.cursor()
        measure_tuple = (measure,)
        cursor.execute('SELECT last_timestamp FROM measurements WHERE measure_name = ?', measure_tuple)
        res = cursor.fetchone()
        insert_tuple = (measure, last_timestamp)
        if res is not None:
            cursor.execute('UPDATE measurements SET last_timestamp = ? WHERE measure_name = ?', insert_tuple)
        else:
            cursor.execute('INSERT INTO measurements (measure_name, last_timestamp) VALUES (?, ?', insert_tuple)
        cursor.commit()
