from db_helpers import init as init_db
from db_helpers import execute_many, SQL_PARAM_CHAR
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


def restructure(data, module):
    n_data = {}
    
    for table in module.Tables:
        n_data[table] = []
        for r in data:
            n_data[table].append([])
            for col in module.IMPORT_MANY_SQL[table]['fields']:
                n_data[table][-1].append(r[col])
                
    return n_data

def set_location(data, loc):
    for r in data:
        r['location'] = loc


def import_chunk(database, data, module, location=None):
    """Given a chunk of data (csv lines) and a module this will execute the SQL to import that data."""
    data = parse_rows(data, module)
    if location is not None:
        set_location(data, "Canada, Alberta, Calgary")

    data = restructure(data, module)

    for table, specs in module.IMPORT_MANY_SQL.items():
        
        statement = "INSERT INTO {}({}) VALUES({})".format(
            specs['table'],
            ','.join(specs['fields']),
            ','.join([SQL_PARAM_CHAR] * len(specs['fields']))
        )
        execute_many(database, statement, data[table])

    
def import_file(database, path, module, location=None, chunk_size=256):
    reader = csv_file(path, fields=module.FIELD_NAMES)

    for chunk in progressbar.progressbar(grouper(reader, chunk_size), max_value=line_count(path)//chunk_size):
        import_chunk(database, chunk, module, location=location)
                        

def main():
    logging.basicConfig(level=logging.WARNING)
    
    database = init_db()
    import_file(database, "weatherstats_calgary_daily.csv", import_daily, location="Calgary, AB, Canada")
    #import_file(database, "weatherstats_calgary_hourly.csv", import_hourly, location="Calgary, AB, Canada")


if __name__ == '__main__':
    main()
