#!/usr/bin/python
"""InfluxMySQLConnector

Usage:
  influx_sql_connector.py [options]

Options:
  -h --help                       Show this screen.
  -d --debug                      Enable debug mode (output logs to screen)
  -t --test-only                  Do not send any data, but run all jobs
  -c <PATH> --config-path <PATH>  Use a custom configuration path
  -j <PATH> --jobs-path <PATH>    Use a custom jobs path
"""

from influxSqlConnector.Runner import Runner
from influxSqlConnector.Mysql import Mysql
from influxSqlConnector.Influx import Influx
from influxSqlConnector.Config import Config
import logging
import os
from docopt import docopt

# InfluxMySQLConnector

def run_connector(config):
    # step 1: read yaml
    # step 2: build queries
    # step 3: get data, chunk it, write json, send to influx
    influx = Influx(config)
    mysql = Mysql(config)
    job_runner = Runner(Config=config, MysqlConnector=mysql, Influx=influx)
    job_runner.run_jobs()

def init_logger():
    log_file = '/var/tmp/log/influx_sql_connector.log'
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))
    log_level = 'DEBUG'
    level_map = {'CRITICAL': logging.CRITICAL,
                 'ERROR': logging.ERROR,
                 'WARNING': logging.WARNING,
                 'INFO': logging.INFO,
                 'DEBUG': logging.DEBUG
                 }
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger('influx_sql_connector')
    root_logger.addHandler(handler)
    root_logger.setLevel(level_map[log_level.upper()])
    root_logger.debug('Initializing logging at {}.'.format(log_file))
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    console.setFormatter(formatter)
    root_logger.addHandler(console)
    return root_logger


if __name__ == "__main__":
    arguments = docopt(__doc__)
    config = Config(arguments)
    # exit()
    logger = init_logger()
    try:
        # exit()
        run_connector(config)
    except Exception, e:
        logger.exception('Exception found')
        print "influx_sql_connector: Fatal error:{}".format(e)
        raise

