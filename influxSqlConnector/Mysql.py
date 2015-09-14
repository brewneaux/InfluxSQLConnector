import pymysql
import pymysql.cursors

class Mysql(object):

    def __init__(self, config):
        self.config = config
        self.connection = pymysql.connect(host=config.mysql_config['host'],
                                user=config.mysql_config['user'],
                                password=config.mysql_config['password'],
                                port=int(config.mysql_config['port']),
                                database=config.mysql_config['database'])
 
    def get_dict_cursor(self):
        return self.connection.cursor(pymysql.cursors.DictCursor)

    def query(self, job, last_measurement):
        if 'custom_query' in job.keys():
            query = self.buildCustomQuery(job, last_measurement)    
            return query
        if job['is_unixtime']:
            query = self.buildUnixTimeQuery(job, last_measurement)
        else:
            query = self.buildDatetimeQuery(job, last_measurement)
        query += ' ORDER BY timestamp '
        return query

    def buildCustomQuery(self, job, last_measurement):
        query = job['custom_query'].replace('\n', ' ')
        if job['is_unixtime']:
            where_clause = ' WHERE {} > {} '.format(job['timestamp_column'], last_measurement)
        else: 
            where_clause = 'WHERE UNIX_TIMESTAMP({}) > {} '.format(job['timestamp_column'], last_measurement)
        query = query.format(where_clause)
        return query

    def buildUnixTimeQuery(self, job, last_measurement):
        query = 'SELECT {} AS timestamp '.format(job['timestamp_column'])
        query += ', {} AS {} '.format(job['measurement'], job['measurement_name'])
        if 'tags' in job.keys():
            for tag, column in job['tags'].iteritems():
                query += ', {} AS {} '.format(column, tag)
        query += 'FROM {} '.format(job['table'])
        query += 'WHERE {} > {} '.format(job['timestamp_column'], last_measurement)
        if 'where' in job.keys():
            query += 'AND {}'.format(job['where'])
        if self.config.test_only is True:
            query += 'LIMIT 0'

        return query

    def buildDatetimeQuery(self, job, last_measurement):
        query = 'SELECT UNIX_TIMESTAMP({}) * 1000 AS timestamp '.format(job['timestamp_column'])
        query += ', {} AS {} '.format(job['measurement'], job['measurement_name'])
        if 'tags' in job.keys():
            for tag, column in job['tags'].iteritems():
                query += ', {} AS {} '.format(column, tag)
        query += 'FROM {} '.format(job['table'])
        query += 'WHERE UNIX_TIMESTAMP({}) > {} '.format(job['timestamp_column'], last_measurement)
        if 'where' in job.keys():
            query += 'AND {}'.format(job['where'])
        if self.config.test_only is True:
            query += 'LIMIT 0'
        return query
