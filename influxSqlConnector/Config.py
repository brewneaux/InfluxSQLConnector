import os
import ConfigParser
import yaml
import logging

class Config(object):
    def __init__(self, arguments):
        self.logger = logging.getLogger('influx_sql_connector')
        self.read_args(arguments)
        self.read_db_config()
        self.read_influx_config()
        self.read_jobs_yaml()

    def read_args(self, arguments):
        self.debug = arguments['--debug']
        self.test_only = arguments['--test-only']
        self.jobs_path = '~/influx_sql_connector/jobs.yaml'
        self.config_path = '~/influx_sql_connector/database_config.ini'
        if arguments['--jobs-path'] is not None:
            self.jobs_path = arguments['--jobs-path']
        if arguments['--config-path'] is not None:
            self.config_path = arguments['--config-path']
        self.jobs_path = os.path.expanduser(self.jobs_path)
        self.config_path = os.path.expanduser(self.config_path)

    def read_db_config(self):
        config = ConfigParser.ConfigParser()
        config.read(self.config_path)
        if 'mysql' not in config.sections():
            raise ValueError("Hey, there is no mysql section in the configuration! Run influx_mysql_config.py first")
        self.mysql_config = config._sections['mysql']

    def read_influx_config(self):
        config = ConfigParser.ConfigParser()
        config.read(self.config_path)
        if 'influx' not in config.sections():
            raise ValueError("Hey, there is no influx section in the configuration! Run influx_mysql_config.py first")
        self.influx_config = config._sections['influx']

    def read_jobs_yaml(self):
        stream = file(self.jobs_path, 'r')
        self.yaml = yaml.safe_load(stream)
        if self.yaml is None:
            raise ValueError('No jobs found in {}'.format(self.jobs_path))
        self.logger.info('Loaded jobs.yaml from {}'.format(self.jobs_path))
