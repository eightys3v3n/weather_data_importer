import tables
import mysql.connector
import csv
import logging
import mysql.connector.errors as SQLErrors
from datetime import datetime

# All defined in main
global database
global LOCATION
global logger 


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
    row = [f if f != '' else None for f in row]
    def get_date(raw):
        date = datetime.strptime(raw, "%Y-%m-%d").date()
        return date

    def imp_temps(date, location, row):
        min_t = row[4]
        max_t = row[1]
        avg_hourly = row[2]
        windchill = row[6]
        db_execute("INSERT INTO tempurature(date, location, min, max, avg_hourly, windchill) VALUES(%s, %s, %s, %s, %s, %s)",
                [date, location, min_t, max_t, avg_hourly, windchill])

    def imp_forecast(date, location, row):
        min_high = row[]
        max_high = row[]
        min_low = row[]
        max_low = row[]
        db_execute("INSERT INTO forecast(date, location, min_high, max_high, min_low, max_low) VALUES(%s, %s, %s, %s, %s, %s)",
                [date, location, min_high, max_high, min_low, max_low])

    def imp_sun(date, location, row):
        sunrise = row[]
        sunset = row[]
        hours_of_light = row[]
        radiation = row[]
        db_execute("INSERT INTO sun(date, location, sunrise, sunset, hours_of_light, radiation) VALUES(%s, %s, %s, %s, %s, %s)",
                [date, location, sunrise, sunset, hours_of_light, radiation])

    date = get_date(row[0])
    location = LOCATION

    imp_temps(date, location, row)
    imp_forecast(date, location, row)
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



def main():
    global LOCATION, database, logger
    LOCATION = "Canada, Alberta, Calgary"

    setup_logger()

    database = tables.init()
    in_file = open("weatherstats_calgary_daily.csv", "r")
    csv_reader = csv.reader(in_file)

    row_length = None # The number of elements in every CSV row. Determined by the header.

    for i, row in enumerate(csv_reader):
        if i == 0:
            row_length = len(row)
            continue
        
        if len(row) != row_length:
            logger.warning("Row is invalid length, ({}): {}".format(len(row), row))
            continue
        
        import_row(row)


if __name__ == '__main__':
    main()
