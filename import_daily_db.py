import tables
import mysql.connector


DEBUG_PRINT_SQL = True


def db_execute(database, cmd, data=None):
    cursor = database.cursor()
    if DEBUG_PRINT_SQL:
        print(cmd, data)
    if data is not None:
        cursor.execute(cmd, data)
    else:
        cursor.execute(cmd)
    return cursor


def main():
    db = tables.init()
    


if __name__ == '__main__':
    main()
