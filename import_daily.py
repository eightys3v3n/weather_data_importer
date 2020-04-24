import mysql.connector
import csv
import logging
import mysql.connector.errors as SQLErrors
from datetime import datetime
import progressbar
import time
import itertools
from collections import defaultdict
from enum import Enum
from db_helpers import init, execute_many


class Tables(Enum):
    Temperature = "temperature"
    Precipitation = "precipitation"

# SQL statement to execute to import a chunk of values
IMPORT_MANY_SQL = {
    Tables.Temperature: {
        'table': "daily_weather.temperature",
        'fields': ['year', 'month', 'day', 'location', 'min', 'max', 'avg_hourly', 'windchill']
    },
    Tables.Precipitation: {
        'table': "daily_weather.precipitation",
        'fields': ['year', 'month', 'day', 'location', 'precipitation', 'rain', 'snow', 'snow_on_ground']
    },
}

# Rename columns with names equal to key to value.
# This will overwrite columns, so be careful.
COLUMN_ALIASES = {'min_temperature': 'min',
                  'max_temperature': 'max',
                  'avg_hourly_temperature': 'avg_hourly',
                  'min_windchill': 'windchill',
}
FIELD_NAMES = ['date','max_temperature','avg_hourly_temperature','avg_temperature','min_temperature','max_humidex','min_windchill','max_relative_humidity','avg_hourly_relative_humidity','avg_relative_humidity','min_relative_humidity','max_dew_point','avg_hourly_dew_point','avg_dew_point','min_dew_point','max_wind_speed','avg_hourly_wind_speed','avg_wind_speed','min_wind_speed','max_wind_gust','wind_gust_dir_10s','max_pressure_sea','avg_hourly_pressure_sea','avg_pressure_sea','min_pressure_sea','max_pressure_station','avg_hourly_pressure_station','avg_pressure_station','min_pressure_station','max_visibility','avg_hourly_visibility','avg_visibility','min_visibility','max_health_index','avg_hourly_health_index','avg_health_index','min_health_index','heatdegdays','cooldegdays','growdegdays_5','growdegdays_7','growdegdays_10','precipitation','rain','snow','snow_on_ground','sunrise','sunset','sunlight','sunrise_f','sunset_f','min_uv_forecast','max_uv_forecast','min_high_temperature_forecast','max_high_temperature_forecast','min_low_temperature_forecast','max_low_temperature_forecast','solar_radiation','max_cloud_cover_4','avg_hourly_cloud_cover_4','avg_cloud_cover_4','min_cloud_cover_4','max_cloud_cover_8','avg_hourly_cloud_cover_8','avg_cloud_cover_8','min_cloud_cover_8','max_cloud_cover_10','avg_hourly_cloud_cover_10','avg_cloud_cover_10','min_cloud_cover_10']
LOCATION = "Calgary, AB, Canada"


def parse_row(row):
    """Converts CSV strings into their correct data types. Also removes invalid entries by returing None"""
    # replace empty string with None
    n_row = {}

    if row is None:
        return None
    
    for k, v in row.items():
        if v == '':
            v = None

        # apply column aliases so the rest of the code can work with SQL table names rather than the long and arbitrary column names.
        if k in COLUMN_ALIASES:
            n_row[COLUMN_ALIASES[k]] = v
        else:
            n_row[k] = v

    if n_row['date'] is None:
        logging.warning("Empty date: '{}'".format(n_row))
        return None
    try:
        n_row['year'] = datetime.strptime(n_row['date'], '%Y-%m-%d').date().year
        n_row['month'] = datetime.strptime(n_row['date'], '%Y-%m-%d').date().month
        n_row['day'] = datetime.strptime(n_row['date'], '%Y-%m-%d').date().day
    except ValueError as e:
        logging.warning("Failed to parse date, '{}' {}".format(e, n_row['date']))
        return None
    del n_row['date']

    if n_row['min'] is not None:
        try:
            n_row['min'] = float(n_row['min'])
        except ValueError as e:
            logging.warning("Failed to parse min, '{}' {}".format(e, n_row['min']))
            n_row['min'] = None
    else:
        logging.info("Row has no min temperature: '{}'".format(n_row))

    if n_row['max'] is not None:
        try:
            n_row['max'] = float(n_row['max'])
        except ValueError as e:
            logging.warning("Failed to parse max, '{}' {}".format(e, n_row['max']))
            n_row['max'] = None
    else:
        logging.info("Row has no max temperature: '{}'".format(n_row))
        
    if n_row['windchill'] is not None:
        try:
            n_row['windchill'] = float(n_row['windchill'])
        except ValueError as e:
            logging.warning("Failed to parse windchill, '{}' {}".format(e, n_row['windchill']))
            n_row['windchill'] = None
    else:
        logging.info("Row has no windchill temperature: '{}'".format(n_row))

    if n_row['avg_hourly'] is  not None:
        try:
            n_row['avg_hourly'] = float(n_row['avg_hourly'])
        except ValueError as e:
            logging.warning("Failed to parse avg_hourly, '{}' {}".format(e, n_row['avg_hourly']))
            n_row['avg_hourly'] = None
    else:
        logging.info("Row has no avg_hourly temperature: '{}'".format(n_row))
        
    n_row['location'] = LOCATION
            
    return n_row
