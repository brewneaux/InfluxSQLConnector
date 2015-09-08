from setuptools import setup

setup(
    # Application name:
    name="influx-sql-connector",

    # Version number (initial):
    version="0.1.0",

    # Application author details:
    author="Jon Bruno",
    author_email="jon@brewneaux.com",

    # Packages
    packages=["influxSqlConnector"],

    # Details
    url="https://github.com/brewneaux/InfluxSQLConnector",

    license="GPL V3",
    description="Connect InfluxDB to your SQL databse.",

    # long_description=open("README.txt").read(),

    # Dependent packages (distributions)
    install_requires=[
        "pymysql",
        "influxdb",
        "docopt"
    ],
)