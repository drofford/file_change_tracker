import daiquiri
import logging as LOG
import os
import sys
import sqlite3

#
# set up logging
#
level = LOG.DEBUG if os.getenv("DEBUG", "") == "Y" else LOG.INFO
LOG.basicConfig(format="%(asctime)s %(levelname)s : %(message)s", level=level)
logger = LOG.getLogger()


def getbasefile():
    """Returns the name of the SQLite DB file"""
    return os.path.splitext(os.path.basename(__file__))[0]


def connectdb() -> sqlite3.Connection:
    """Connects to the SQLite DB"""
    try:
        dbfile = getbasefile() + ".db"
        logger.debug('connectdb: DB file name is "{}"'.format(dbfile))
        conn = sqlite3.connect(dbfile, timeout=2)
        logger.debug("connectdb: connected w/ conn = {}".format(conn))
        return conn
    except Exception as ex:
        logger.error("connectdb: Error: exception {} caught".format(ex))
        sys.stderr.flush()
        exit(1)


def tableexists(table: str) -> bool:
    """Checks if a SQLite DB Table exists"""
    result = False
    try:
        conn = connectdb()
        if conn is not None:
            logger.debug(
                "tableexists: Connected to database with connection = {}".format(conn)
            )

            query = "SELECT count(id) from {}".format(table)  # Finish this query ...
            args = (table,)

            result = corecursor(conn, query)  # , args)
            logger.debug("result = {}".format(result))

            return False

    except Exception as ex:
        logger.error('tableexists: exception of type "{}" caught'.format(type(ex)))
        logger.error('tableexists: exception "{}" caught'.format(ex))
        sys.stderr.flush()
        return False


def corecursor(conn: sqlite3.Connection, query: str, args: list = None) -> bool:
    try:
        cursor = conn.cursor()
        result = cursor.execute(query, args)
        logger.debug("corecursor: result = {}".format(result))
        return True
    except sqlite3.OperationalError as ex:
        logger.error('corecursor[1]: exception of type "{}" caught'.format(type(ex)))
        logger.error('corecursor[1]: Error: exception "{}" caught'.format(ex))
        return False
    except Exception as ex:
        logger.error('corecursor[2]: exception of type "{}" caught'.format(type(ex)))
        logger.error('corecursor[2]: Error: exception "{}" caught'.format(ex))
        sys.stderr.flush()
        exit(1)


def createhashtable(table: str = "files") -> bool:
    result = False
    query = (
        "CREATE TABLE {} if not exist (id integer primary key, file_name text)".format(
            table
        )
    )
    try:
        conn = connectdb()
        if conn is not None:
            if tableexists(table):
                logger.debug("createhashtable: table {} does exist".format(table))
            else:
                logger.debug(
                    "createhashtable: table {} does not exist (yet)".format(table)
                )
                try:
                    cursor = conn.cursor()
                    result = cursor.execute(query)
                    logger.debug("createhashtable: result = {}".format(result))
                    # To be continued ...
                except Exception as ex:
                    logger.error(
                        'createhashtable[1]: Error: exception "{}" caught'.format(ex)
                    )
                    exit(1)
    except Exception as ex:
        logger.error('createhashtable[2]: Error: exception "{}" caught'.format(ex))
        sys.stderr.flush()
        exit(1)
    return result


def main():
    FILE_TABLE_NAME = "files"
    table_exists = tableexists(FILE_TABLE_NAME)
    logger.debug(
        'main: table "{}" does{} exist'.format(
            FILE_TABLE_NAME, "" if table_exists else " not"
        )
    )
    if not table_exists:
        result = createhashtable(FILE_TABLE_NAME)


#    cursor = conn.cursor()
#
#    cursor.execute("create table if not exists master (id integer primary key, filename text, last_accessed datetime)")
#    conn.commit()

if __name__ == "__main__":
    main()
