from influxdb import InfluxDBClient
from dateutil.parser import parse
import time
import ConfigParser

class Influx(object):
    def __init__(self):
        config = ConfigParser.ConfigParser()
        config.read('database_config.ini')
        if 'influx' not in config.sections():
            raise ValueError("Hey, there is no configuration! Run influx_mysql_config.py first")
        self.influxClient = InfluxDBClient(config.get('influx', 'host'),
                                           config.get('influx', 'port'),
                                           config.get('influx', 'user'),
                                           config.get('influx', 'password'),
                                           config.get('influx', 'db'))

    # this doesn't work because Influx doesnt allow it to work
    def getLastMeasurement(self, Measure):
        result = self.influxClient.query('SELECT * from {} ORDER BY time LIMIT 1'.format(Measure))
        if result.__len__() is 0:
            return 0
        for points in result.get_points():
            point = points
        timeTuple = parse(point['time']).timetuple()
        return int(time.mktime(timeTuple))

    def sendRow(self, job, rowData):
        # build JSON here.
        pointValues = self.buildDict(job, rowData)
        self.influxClient.write_points(pointValues, time_precision='s')

    def buildDict(self, job, rowData):
        tags = {}
        if 'tags' in job.keys():
            i = 2
            for tag, column in job['tags'].iteritems():
                tags[tag] = rowData[i]
                i += 1
        pointValues = [{
                "time": rowData[0],
                "measurement": job['measurement'],
                'fields': {
                    'value': rowData[1],
                },
                'tags': tags
                }]
        return pointValues