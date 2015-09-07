import yaml
import os
import pprint

class runner(object):

    def __init__(self, yaml_path='~/.influx_mysql_connector.yaml', MysqlConnector=None, Influx=None):
        if (MysqlConnector is None or Influx is None):
            raise ValueError('Missing connection to database')
        self.MysqlConnector = MysqlConnector
        self.Influx = Influx
        self.loadJobs(yaml_path)
        self.pp =  pprint.PrettyPrinter(indent=4)

    def loadJobs(self, yaml_path):
        self.yaml_location = os.path.expanduser(yaml_path)
        stream = file(self.yaml_location, 'r')
        self.yaml = yaml.safe_load(stream)
        if self.yaml is None:
            raise ValueError('No jobs found in {}'.format(self.yaml_location))

    def runJobs(self):
        for job in self.yaml:
            self.pp.pprint(job)
            measure = job['measurement']
            lastMeasure = self.Influx.getLastMeasurement(measure)
            query = self.MysqlConnector.query(job, lastMeasure)
            self.sendPoints(job, query)

    def sendPoints(self, job, query):
        dataCursor = self.MysqlConnector.connection.cursor()
        dataCursor.execute(query)
        total = dataCursor.rowcount
        if total > 1000:
            print("This may take a while...")
        print("Insert {} points...".format(total))
        row = dataCursor.fetchone()
        while row is not None:
            self.Influx.sendRow(job, row)
            row = dataCursor.fetchone()
