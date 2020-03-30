import mysql.connector

def db_execute(db, cmd, data=None):
    cursor = db.cursor()
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


def db_execute_many(db, cmd, data):
    cursor = db.cursor()
    logger.debug("SQL command: {}\n\t\tSQL data: {}".format(cmd, data))
    try:
        cursor.executemany(cmd, data)
    except SQLErrors.IntegrityError as e:
        if not e.__str__().startswith("1062 (23000): Duplicate entry"):
            raise e
    return cursor
