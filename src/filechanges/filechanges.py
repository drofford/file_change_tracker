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


def corecursor(conn: sqlite3.Connection, query: str, args: list = None) -> bool:
    result = False
    cursor = conn.cursor()
    try:
        if args is None:
            debug("invoking cursor.execute() without args")
            r = cursor.execute(query)
            debug(f"cursor.execute() returned {r}")
        else:
            debug("invoking cursor.execute() with args")
            r = cursor.execute(query, args)
            debug(f"cursor.execute() returned {r}")
        rows = cursor.fetchall()
        numrows = len(list(rows))
        debug(f"{numrows=}")
        if numrows > 0:
            result = True
    except sqlite3.OperationalError as err:
        error(str(err))
    finally:
        cursor.close()

    debug(f"returning {result}")
    return result


def tableexists(table_name: str) -> bool:
    result = False
    conn = connectdb()
    if conn is not None:
        try:
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
            args = (table_name,)
            result = corecursor(conn, query, args)
        except sqlite3.OperationalError as err:
            error(str(err))
        finally:
            conn.close()

    return result


def createhashtable(table: str = "files") -> bool:
    result = False
    try:
        conn = connectdb()
        if conn is not None:
            r = tableexists(table)
            debug(f"{r=}")
            if r:
                debug(f"table {table} does exist")
            else:
                debug(f"table {table} does not exist")

                try:
                    query = f"CREATE TABLE IF NOT EXISTS {table} (id integer primary key, file_name text)"
                    args = (table,)
                    corecursor(conn, query, None)
                    result = True
                except sqlite3.OperationalError as err:
                    error(str(err))
                finally:
                    conn.close()
    except sqlite3.OperationalError as ex:
        debug('exception of type "{}" caught: {}'.format(type(ex), ex))
    except Exception as ex:
        error(f'caught exception of type {type(ex)}: "{ex}", program abending')
        exit(1)

    debug(f"returning {result}")
    return result
