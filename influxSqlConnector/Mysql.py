import pymysql

class Mysql(object):

    def __init__(self, config):
        self.config = config
        self.connection = pymysql.connect(host=config.mysql_config['host'],
                                user=config.mysql_config['user'],
                                password=config.mysql_config['password'],
                                port=int(config.mysql_config['port']),
                                database=config.mysql_config['database'])

    def query(self, job, last_measurement):
        if job['is_unixtime']:
            query = self.buildUnixTimeQuery(job, last_measurement)
        else:
            query = self.buildDatetimeQuery(job, last_measurement)
        return query

    def buildUnixTimeQuery(self, job, last_measurement):
        query = 'SELECT {} AS timestamp '.format(job['timestamp_column'])
        query += ', {} AS {} '.format(job['measurement'], job['measurement_name'])
        if 'tags' in job.keys():
            for tag, column in job['tags'].iteritems():
                query += ', {} AS {} '.format(column, tag)
        query += 'FROM {} '.format(job['table'])
        query += 'WHERE {} > {} '.format(job['timestamp_column'], last_measurement)
        if self.config.test_only is True:
            query += 'LIMIT 0'
        return query

    def buildDatetimeQuery(self, job, last_measurement):
        query = 'SELECT UNIX_TIMESTAMP({}) AS timestamp '.format(job['timestamp_column'])
        query += ', {} AS {} '.format(job['measurement'], job['measurement_name'])
        if 'tags' in job.keys():
            for tag, column in job['tags'].iteritems():
                query += ', {} AS {} '.format(column, tag)
        query += 'FROM {} '.format(job['table'])
        query += 'WHERE UNIX_TIMESTAMP({}) > {} '.format(job['timestamp_column'], last_measurement)
        if self.config.test_only is True:
            query += 'LIMIT 0'
        return query
