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

    def send_row(self, job, row_data, timestamp):
        point_values = self.build_dict(job, row_data, timestamp)
        self.influx_client.write_points(point_values, time_precision='ms')

    def build_dict(self, job, row_data, timestamp):
        point_values = {
            "time": timestamp,
            "measurement": job['measurement'],
            'fields': {}
        }
        if 'fields' in job.keys():
            for field_name, column_name in job['fields'].iteritems():
                point_values['fields'][field_name] = self.convert_string(row_data[column_name])
        else:
            point_values['fields']['value'] = self.convert_string(row_data[job['measurement_name']])
        return [point_values]

    def convert_string(self, string_to_convert):
        if type(string_to_convert) is str:
            return float(string_to_convert) if '.' in string_to_convert else int(string_to_convert)
        else:
            return string_to_convert

    # def build_dict(self, job, row_data, timestamp):
    #     tags = {}
    #     if 'tags' in job.keys():
    #         i = 2
    #         for tag, column in job['tags'].iteritems():
    #             tags[tag] = row_data[i]
    #             i += 1
    #     point_values = [{
    #             "time": timestamp,
    #             "measurement": job['measurement'],
    #             'fields': {
    #                 'value': row_data[1],
    #             },
    #             'tags': tags
    #             }]
    #     return point_values
