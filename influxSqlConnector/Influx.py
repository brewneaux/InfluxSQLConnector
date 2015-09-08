from influxdb import InfluxDBClient
from dateutil.parser import parse
import time

class Influx(object):
    def __init__(self, config):
        self.config = config
        self.influx_client = InfluxDBClient(config.influx_config['host'],
                                            config.influx_config['port'],
                                            config.influx_config['user'],
                                            config.influx_config['password'],
                                            config.influx_config['db'])

    # this doesn't work because Influx doesnt allow it to work
    def get_last_measurement(self, Measure):
        result = self.influx_client.query('SELECT * from {} ORDER BY time LIMIT 1'.format(Measure))
        if result.__len__() is 0:
            return 0
        for points in result.get_points():
            point = points
        time_tuple = parse(point['time']).timetuple()
        return int(time.mktime(time_tuple))

    def send_row(self, job, row_data):
        point_values = self.build_dict(job, row_data)
        self.influx_client.write_points(point_values, time_precision='s')

    def build_dict(self, job, row_data):
        tags = {}
        if 'tags' in job.keys():
            i = 2
            for tag, column in job['tags'].iteritems():
                tags[tag] = row_data[i]
                i += 1
        point_values = [{
                "time": row_data[0],
                "measurement": job['measurement'],
                'fields': {
                    'value': row_data[1],
                },
                'tags': tags
                }]
        return point_values
