
import logging

class Runner(object):

    def __init__(self, Config, MysqlConnector=None, Influx=None):
        self.logger = logging.getLogger('influx_sql_connector')
        if (MysqlConnector is None or Influx is None):
            raise ValueError('Missing connection to database')
        self.MysqlConnector = MysqlConnector
        self.Influx = Influx
        self.config = Config

    def run_jobs(self):
        for job_number, job in enumerate(self.config.yaml):
            if self.job_validator(job_number, job) is False:
                continue
            self.logger.info('Running job for measurement: "{}"'.format(job['measurement']))
            measure = job['measurement']
            last_measure = self.Influx.get_last_measurement(measure)
            query = self.MysqlConnector.query(job, last_measure)
            self.send_points(job, query)

    def send_points(self, job, query):
        data_cursor = self.MysqlConnector.connection.cursor()
        data_cursor.execute(query)
        total = data_cursor.rowcount
        if total is 0:
            self.logger.error('No points to insert for "{}"'.format(job['measurement']))
        if total > 1000:
            print("This may take a while...")
        self.logger.info('Inserting {} points for "{}"'.format(total, job['measurement']))
        row = data_cursor.fetchone()
        while row is not None:
            self.Influx.send_row(job, row)
            row = data_cursor.fetchone()

    def job_validator(self, job_number, job):
        # required fields: table, timestamp_column, is_unixtime, measurement, measurement_column, measurement_name
        required_keys = [
            'timestamp_column',
            'table',
            'measurement_column',
            'measurement',
            'measurement_name',
            'is_unixtime'
        ]
        if not all(key in job.keys() for key in required_keys):
            job_number += 1
            if 'measurement' in job.keys():
                self.logger.error('Missing a required key from the job config for job number {}, "{}"'.format(job_number, job['measurement']))
                return False
            else:
                self.logger.error("Missing a required key from a job config section job number {}".format(job_number))
                return False
