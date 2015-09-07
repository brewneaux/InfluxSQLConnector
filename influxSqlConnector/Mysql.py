import pymysql
import ConfigParser

class Mysql(object):

    def __init__(self):
        config = ConfigParser.ConfigParser()
        config.read('database_config.ini')
        if 'mysql' not in config.sections():
            raise ValueError("Hey, there is no configuration! Run influx_mysql_config.py first")
        self.connection = pymysql.connect(host=config.get('mysql', 'host'),
                                user=config.get('mysql', 'user'),
                                password=config.get('mysql', 'password'),
                                port=int(config.get('mysql', 'port')),
                                database=config.get('mysql', 'database'))

    def query(self, job, lastMeasurement):
        if job['is_unixtime']:
            query = self.buildUnixTimeQuery(job, lastMeasurement)
        else:
            query = self.buildDatetimeQuery(job, lastMeasurement)
        return query

    def buildUnixTimeQuery(self, job, lastMeasurement):
        query = 'SELECT {} AS timestamp '.format(job['timestamp_column'])
        query += ', {} AS {} '.format(job['measurement'], job['measurement_name'])
        if 'tags' in job.keys():
            for tag, column in job['tags'].iteritems():
                query += ', {} AS {} '.format(column, tag)
        query += 'FROM {} '.format(job['table'])
        query += 'WHERE {} > {} '.format(job['timestamp_column'], lastMeasurement)
        return query

    def buildDatetimeQuery(self, job, lastMeasurement):
        query = 'SELECT UNIX_TIMESTAMP({}) AS timestamp '.format(job['timestamp_column'])
        query += ', {} AS {} '.format(job['measurement'], job['measurement_name'])
        if 'tags' in job.keys():
            for tag, column in job['tags'].iteritems():
                query += ', {} AS {} '.format(column, tag)
        query += 'FROM {} '.format(job['table'])
        query += 'WHERE UNIX_TIMESTAMP({}) > {} '.format(job['timestamp_column'], lastMeasurement)
        return query
