from db_helpers import init as init_db
from db_helpers import execute_many
from helpers import csv_file, grouper, line_count
from collections import defaultdict
import logging
import import_daily
import progressbar


def parse_rows(data, module):
    """Converts data types of all rows and removes invalid rows."""
    n_data = []
    for d in data:
        r = module.parse_row(d)
        if r is not None:
            n_data.append(r)
    return n_data


def restructure(data):
    n_data = []
    
    for r in data:
        n_data.append((r['year'], r['month'], r['day'], r['location'], r['min'], r['max'], r['avg_hourly'], r['windchill']
            ))

    return n_data

def set_location(data, loc):
    for r in data:
        r['location'] = loc


def import_chunk(database, data, module, location=None):
    """Given a chunk of data (csv lines) and a module this will execute the SQL to import that data."""
    data = parse_rows(data, module)
    if location is not None:
        set_location(data, "Canada, Alberta, Calgary")
    data = restructure(data)
    execute_many(database, module.IMPORT_MANY_SQL, data)

    
def import_file(database, path, module, location=None, chunk_size=1024):
    reader = csv_file(path, fields=module.FIELD_NAMES)

    for chunk in progressbar.progressbar(grouper(reader, chunk_size), max_value=line_count(path)/chunk_size):
        import_chunk(database, chunk, module, location=location)
                        

def main():
    logging.basicConfig(level=logging.WARNING)
    
    database = init_db()
    import_file(database, "weatherstats_calgary_daily.csv", import_daily, location="Calgary, AB, Canada")
    #import_file(database, "weatherstats_calgary_hourly.csv", import_hourly, location="Calgary, AB, Canada")


if __name__ == '__main__':
    main()
