import mysql.connector
import json


TABLES = (
"""CREATE TABLE IF NOT EXISTS
tempurature(
    date        DATE NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    windchill   FLOAT,
    PRIMARY KEY (date, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
forecast(
    date        DATE NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min_high    FLOAT,
    max_high    FLOAT,
    min_low     FLOAT,
    max_low     FLOAT,
    PRIMARY KEY (date, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
sun(
    date            DATE NOT NULL,
    location        VARCHAR(64) NOT NULL,
    sunrise         TIME,
    sunset          TIME,
    hours_of_light  FLOAT,
    radiation       FLOAT,
    PRIMARY KEY (date, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
humidity(
    date        DATE NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    PRIMARY KEY (date, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
wind(
    date        DATE NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    max_gust    FLOAT,
    gust_length FLOAT,
    PRIMARY KEY (date, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
pressure(
    date                DATE NOT NULL,
    location            VARCHAR(64) NOT NULL,
    max_station         FLOAT,
    min_station         FLOAT,
    max_sea             FLOAT,
    min_sea             FLOAT,
    avg_hourly_station  FLOAT,
    avg_hourly_sea      FLOAT
)
""",
"""CREATE TABLE IF NOT EXISTS
precipitation(
    date            DATE NOT NULL,
    location        VARCHAR(64),
    precipitation   FLOAT,
    rain            FLOAT,
    snow            FLOAT,
    snow_on_ground  FLOAT,
    PRIMARY KEY (date, location)
)
"""
)
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S"
CONFIG_PATH = "secrets.json"


def read_config(path):
    data = json.loads(open(path, 'r').read())
    return data


def open_db(sql_conf):
    user = sql_conf['user']
    passwd = sql_conf['password']
    db = sql_conf['database']

    database = mysql.connector.connect(host="127.0.0.1", user=user, passwd=passwd, database=db, autocommit=True)
    return database


def create_tables(db, tables):
    cursor = db.cursor()
    for table in tables:
        cursor.execute(table)


def init():
    data = read_config(CONFIG_PATH)
    sql_conf = data['sql']

    db = open_db(sql_conf)
    create_tables(db, TABLES)
    return db


def main():
    init()


if __name__ == '__main__':
    main()
