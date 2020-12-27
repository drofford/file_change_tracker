import daiquiri
import inspect
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


def debug(msg):
    f = inspect.currentframe()
    logger.debug(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def info(msg):
    f = inspect.currentframe()
    logger.info(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def error(msg):
    f = inspect.currentframe()
    logger.error(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def fatal(msg):
    f = inspect.currentframe()
    logger.error(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")
    exit(1)


def getbasefile():
    """Returns the name of the SQLite DB file"""
    return os.path.splitext(os.path.basename(__file__))[0]


def connectdb() -> sqlite3.Connection:
    """Connects to the SQLite DB"""
    try:
        dbfile = getbasefile() + ".db"
        debug('DB file name is "{}"'.format(dbfile))
        conn = sqlite3.connect(dbfile, timeout=2)
        debug("connected w/ conn = {}".format(conn))
        return conn
    except Exception as ex:
        error("exception {} caught".format(ex))
        sys.stderr.flush()
        exit(1)


def tableexists(table: str) -> bool:
    """Checks if a SQLite DB Table exists"""
    result = False
    try:
        conn = connectdb()
        if conn is not None:
            query = f"SELECT id from {table}"  # Finish this query ...
            debug(f"{query=}")

            result = corecursor(conn, query)
            debug(f"{result=}")

            return result

    except Exception as ex:
        error('exception "{}" caught'.format(ex))
        return False


def corecursor(conn: sqlite3.Connection, query: str, args: list = None) -> bool:
    try:
        cursor = conn.cursor()
        if args is None:
            result = cursor.execute(query)
        else:
            result = cursor.execute(query, args)
        debug(f"result = {result}, returning True")
        return True
    except sqlite3.OperationalError as ex:
        debug(f'caught exception of type {type(ex)}: "{ex}", returning False')
        return False
    except Exception as ex:
        error(f'caught exception of type {type(ex)}: "{ex}", program abending')
        exit(1)


def createhashtable(table: str = "files") -> bool:
    result = False
    try:
        conn = connectdb()
        if conn is not None:
            if tableexists(table):
                debug(f"table {table} does exist")
            else:
                debug(f"table {table} does not exist")
                query = f"CREATE TABLE IF NOT EXISTS {table} (id integer primary key, file_name text)"
                debug(f"Create table statement = {query}")

                args = (table,)
                try:
                    cursor = conn.cursor()
                    result = cursor.execute(query)
                    conn.commit()
                    debug("result = {}".format(result))
                    result = True
                except sqlite3.OperationalError as ex:
                    debug('exception of type "{}" caught: {}'.format(type(ex), ex))
                except Exception as ex:
                    error('exception "{}" caught'.format(ex))
                    exit(1)
    except sqlite3.OperationalError as ex:
        debug('exception of type "{}" caught: {}'.format(type(ex), ex))
    except Exception as ex:
        error(f'caught exception of type {type(ex)}: "{ex}", program abending')
        exit(1)
    return result
