import tables
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
from database_helpers import db_execute_many

# All defined in main
global LOCATION
global logger 


class Field(Enum):
    Temps = "temperature"


def import_data(database, rows):
    db_execute_many(database,
                    "INSERT INTO daily_temp(date, location, min, max, avg_hourly, windchill) VALUES(%s, %s, %s, %s, %s, %s)",
            rows[Field.Temps])


def parse_row(row):
    data = {}
    #row = [f if f != '' else None for f in row]
    # Replace empty strings with None
    n_row = {}
    for k, v in row.items():
        if v == '':
            v = None
        n_row[k] = v
    row = n_row

    def get_date(raw):
        date = datetime.strptime(raw, "%Y-%m-%d").date()
        return date

    def parse_temps(date, location, row):
        min_t = row['min_temperature'] # 4
        max_t = row['max_temperature'] # 1
        avg_hourly = row['avg_hourly_temperature'] # 2
        windchill = row['min_windchill'] # 6
        #db_execute("INSERT INTO tempurature(date, location, min, max, avg_hourly, windchill) VALUES(%s, %s, %s, %s, %s, %s)",
        return [date, location, min_t, max_t, avg_hourly, windchill]

    date = get_date(row['date']) # 0
    location = LOCATION

    data[Field.Temps] = parse_temps(date, location, row)
    return data


def grouper(iterable, n):
    args = [iter(enumerate(iterable))] * n
    return itertools.zip_longest(*args)


def import_rows(iterable, entries, chunk_size):
    with progressbar.ProgressBar(max_value=entries) as bar:
        for chunk in grouper(iterable, chunk_size):
            data = defaultdict(lambda: [])

            for r in chunk:
                if r is None: continue
                i, row = r
            
                for k, v in parse_row(row).items():
                    data[k].append(v)

                bar.update(i)

            data = dict(data)
            import_data(data)


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


def import_daily(database, csv_path):
    in_file = open(csv_path, "r")
    csv_reader = csv.DictReader(in_file)
    
    start = time.time()

    import_rows(csv_reader, get_lines(csv_path), 1024)

    end = time.time()
    print("Daily import took", end-start, "seconds")


def main():
    global LOCATION, logger
    LOCATION = "Canada, Alberta, Calgary"

    setup_logger()

    database = tables.init()
    import_daily(database, "weatherstats_calgary_daily.csv")
    


if __name__ == '__main__':
    main()
