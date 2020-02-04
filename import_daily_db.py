import tables
import mysql.connector
import csv
import logging
import mysql.connector.errors as SQLErrors
from datetime import datetime
import progressbar
import time

# All defined in main
global database
global LOCATION
global logger 
global WEATHER_CSV

def db_execute(cmd, data=None):
    cursor = database.cursor()
    logger.debug("SQL command: {}\n\t\tSQL data: {}".format(cmd, data))
    try:
        if data is not None:
            cursor.execute(cmd, data)
        else:
            cursor.execute(cmd)
    except SQLErrors.IntegrityError as e:
        if not e.__str__().startswith("1062 (23000): Duplicate entry"):
            raise e
    return cursor


def import_row(row):
    #row = [f if f != '' else None for f in row]
    n_row = {}
    for k, v in row.items():
        if v == '':
            v = None
        n_row[k] = v
    row = n_row

    def get_date(raw):
        date = datetime.strptime(raw, "%Y-%m-%d").date()
        return date

    def imp_temps(date, location, row):
        min_t = row['min_temperature'] # 4
        max_t = row['max_temperature'] # 1
        avg_hourly = row['avg_hourly_temperature'] # 2
        windchill = row['min_windchill'] # 6
        db_execute("INSERT INTO tempurature(date, location, min, max, avg_hourly, windchill) VALUES(%s, %s, %s, %s, %s, %s)",
                [date, location, min_t, max_t, avg_hourly, windchill])

    def imp_forecast(date, location, row):
        min_high = row[53]
        max_high = row[54]
        min_low = row[55]
        max_low = row[56]
        db_execute("INSERT INTO forecast(date, location, min_high, max_high, min_low, max_low) VALUES(%s, %s, %s, %s, %s, %s)",
                [date, location, min_high, max_high, min_low, max_low])

    def imp_sun(date, location, row):
        sunrise = row[46]
        sunset = row[47]
        hours_of_light = row[48]
        radiation = row[57]
        db_execute("INSERT INTO sun(date, location, sunrise, sunset, hours_of_light, radiation) VALUES(%s, %s, %s, %s, %s, %s)",
                [date, location, sunrise, sunset, hours_of_light, radiation])

    date = get_date(row['date']) # 0
    location = LOCATION

    imp_temps(date, location, row)
    #imp_forecast(date, location, row)
    #imp_sun(row)
    #imp_humid(row)
    #imp_wind(row)
    #imp_pressure(row)
    #imp_precip(row)


def setup_logger():
    global logger
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('[%(levelname)s] %(name)s: %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)


def get_lines(path):
    total_lines = 0
    with open(path, 'r') as f:
        for line in f:
            total_lines += 1
    return total_lines


def main():
    global LOCATION, database, logger, WEATHER_CSV
    LOCATION = "Canada, Alberta, Calgary"
    WEATHER_CSV = "weatherstats_calgary_daily.csv"

    setup_logger()

    database = tables.init()
    in_file = open(WEATHER_CSV, "r")
    #csv_reader = csv.DictReader(in_file)
    csv_reader = csv.DictReader(in_file)

    row_length = None # The number of elements in every CSV row. Determined by the header.
    
    start = time.time()
    for i, row in progressbar.progressbar(enumerate(csv_reader), max_value=get_lines(WEATHER_CSV)):
        if i == 0:
            row_length = len(row)
            continue
            
        if len(row) != row_length:
            logger.warning("Row is invalid length, ({}): {}".format(len(row), row))
            continue
        
        import_row(row)

    end = time.time()
    print("Length:", end-start, "seconds")


if __name__ == '__main__':
    main()
