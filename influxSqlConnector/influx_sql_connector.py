#!/usr/bin/python

from runner import runner
from Mysql import Mysql
from Influx import Influx

# InfluxMySQLConnector

def runConnector():
    # step 1: read yaml
    # step 2: build queries
    # step 3: get data, chunk it, write json, send to influx
    influx = Influx()
    mysql = Mysql()
    jobRunner = runner(MysqlConnector=mysql, Influx=influx)
    jobRunner.runJobs()

if __name__ == "__main__":
    runConnector()
