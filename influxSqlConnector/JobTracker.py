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

    def get_last_measurement(self, measure):
        cursor = self.sqlite.cursor()
        measure_tuple = (measure,)
        cursor.execute('SELECT last_timestamp FROM measurements WHERE measure_name = ?', measure_tuple)
        res = cursor.fetchone()
        cursor.close()
        if res is None:
            return 0
        else:
            return int(res[0])


    def insert_measure(self, measure, last_timestamp):
        last_timestamp = last_timestamp / 1000
        cursor = self.sqlite.cursor()
        measure_tuple = (measure,)
        cursor.execute('SELECT last_timestamp FROM measurements WHERE measure_name = ?', measure_tuple)
        res = cursor.fetchone()
        insert_tuple = [measure, last_timestamp]
        if res is not None:
            try:
                update_cur = self.sqlite.cursor()
                update_query = 'UPDATE measurements SET last_timestamp = {} WHERE measure_name = "{}"'.format(last_timestamp, measure)
                self.sqlite.execute(update_query)
                self.sqlite.commit()
            except sqlite3.Error as er:
                print 'er:', er.message
        else:
            cursor.execute('INSERT INTO measurements (measure_name, last_timestamp) VALUES (?, ?)', insert_tuple)
        self.sqlite.commit()

    def close_connection(self):
        self.sqlite.close()