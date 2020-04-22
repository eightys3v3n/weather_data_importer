import mysql.connector
import json
import logging


TABLES = (
"""CREATE DATABASE IF NOT EXISTS daily_weather""",
"""CREATE TABLE IF NOT EXISTS
daily_weather.temperature(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    windchill   FLOAT,
    PRIMARY KEY (year, month, day, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
daily_weather.forecast(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min_high    FLOAT,
    max_high    FLOAT,
    min_low     FLOAT,
    max_low     FLOAT,
    PRIMARY KEY (year, month, day, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
daily_weather.sun(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    location        VARCHAR(64) NOT NULL,
    sunrise         TIME,
    sunset          TIME,
    hours_of_light  FLOAT,
    radiation       FLOAT,
    PRIMARY KEY (year, month, day, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
daily_weather.humidity(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    PRIMARY KEY (year, month, day, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
daily_weather.wind(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    max_gust    FLOAT,
    gust_length FLOAT,
    PRIMARY KEY (year, month, day, location)
)
""",
"""CREATE TABLE IF NOT EXISTS
daily_weather.pressure(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
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
daily_weather.precipitation(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    location        VARCHAR(64),
    precipitation   FLOAT,
    rain            FLOAT,
    snow            FLOAT,
    snow_on_ground  FLOAT,
    PRIMARY KEY (year, month, day, location)
)
""",


"""CREATE DATABASE IF NOT EXISTS hourly_weather""",
"""CREATE TABLE IF NOT EXISTS
hourly_weather.temperature(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    hour        INT NOT NULL,
    minute      INT NOT NULL,
    second      FLOAT NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    PRIMARY KEY (year, month, day, hour, minute, second, location)
)""",
"""CREATE TABLE IF NOT EXISTS
hourly_weather.humidity(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    hour        INT NOT NULL,
    minute      INT NOT NULL,
    second      FLOAT NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    PRIMARY KEY (year, month, day, hour, minute, second, location)
)""",
"""CREATE TABLE IF NOT EXISTS
hourly_weather.wind(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    hour        INT NOT NULL,
    minute      INT NOT NULL,
    second      FLOAT NOT NULL,
    location    VARCHAR(64) NOT NULL,
    min         FLOAT,
    max         FLOAT,
    avg_hourly  FLOAT,
    max_gust    FLOAT,
    gust_length FLOAT,
    PRIMARY KEY (year, month, day, hour, minute, second, location)
)""",
"""CREATE TABLE IF NOT EXISTS
hourly_weather.pressure(
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    hour        INT NOT NULL,
    minute      INT NOT NULL,
    second      FLOAT NOT NULL,
    location  VARCHAR(64) NOT NULL,
    station   FLOAT,
    sea_level FLOAT,
    PRIMARY KEY (year, month, day, hour, minute, second, location)
)"""
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
    #db = sql_conf['database']

    database = mysql.connector.connect(host="127.0.0.1", user=user, passwd=passwd, autocommit=True)
    return database


def execute(db, cmd, data=None):
    cursor = db.cursor()
    logging.debug("SQL command: {}\n\t\tSQL data: {}".format(cmd, data))
    try:
        if data is not None:
            cursor.execute(cmd, data)
        else:
            cursor.execute(cmd)
    except mysql.connector.errors.IntegrityError as e:
        if not e.__str__().startswith("1062 (23000): Duplicate entry"):
            raise e
    return cursor


def execute_many(db, cmd, data):
    cursor = db.cursor()
    logging.debug("SQL command: {}\n\t\tSQL data: {}".format(cmd, data))
    try:
        cursor.executemany(cmd, data)
    except mysql.connector.errors.IntegrityError as e:
        if not e.__str__().startswith("1062 (23000): Duplicate entry"):
            raise e
    return cursor


def init():
    data = read_config(CONFIG_PATH)
    sql_conf = data['sql']

    db = open_db(sql_conf)
    for table in TABLES:
        execute(db, table)
    return db


def main():
    init()


if __name__ == '__main__':
    main()
