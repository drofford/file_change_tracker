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
            debug(
                "Connected to database with connection = {}".format(conn)
            )

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
        debug(f"A: {conn=}, {query=}")
        cursor = conn.cursor()
        debug(f"B: {cursor=}")
        if args is None:
            result = cursor.execute(query)
        else:
            result = cursor.execute(query, args)
        debug(f"C")
        debug(f"result = {result}, returning True")
        return True
    except sqlite3.OperationalError as ex:
        debug(
            f'caught exception of type {type(ex)}: "{ex}", returning False'
        )
        return False
    except Exception as ex:
        error(
            f'caught exception of type {type(ex)}: "{ex}", returning None'
        )
        # exit(1)
        return None


def createhashtable(table: str = "files") -> bool:
    result = False
    try:
        debug("AAA.1")
        conn = connectdb()
        debug("AAA.2")
        if conn is not None:
            debug("BBB.1")
            if tableexists(table):
                debug("BBB.1.A")
                debug("table {} does exist".format(table))
            else:
                debug("BBB.2.A")
                debug("table {} does not exist (yet)".format(table)
                )
                QUERY = (
                    "CREATE TABLE IF NOT EXISTS "
                    + table
                    + " (id integer primary key, file_name text)"
                )
                debug(f"Query = {QUERY}")

                args = (table,)
                try:
                    debug("BBB.2.B")
                    cursor = conn.cursor()
                    debug("BBB.2.C")
                    result = cursor.execute(QUERY)
                    debug("BBB.2.D")
                    conn.commit()
                    debug("BBB.2.E")
                    debug("result = {}".format(result))
                    result = True
                    debug("BBB.2.F")
                except sqlite3.OperationalError as ex:
                    debug(
                        'exception of type "{}" caught: {}'.format(
                            type(ex), ex
                        )
                    )
                except Exception as ex:
                    error(
                        'exception "{}" caught'.format(ex)
                    )
                    exit(1)
    except sqlite3.OperationalError as ex:
        debug(
            'exception of type "{}" caught: {}'.format(type(ex), ex)
        )
    except Exception as ex:
        error('exception "{}" caught'.format(ex))
        exit(1)
    debug(f"CCC: result = {result}")
    return result


def main():
    FILE_TABLE_NAME = "files"
    table_exists = tableexists(FILE_TABLE_NAME)
    debug(
        'table "{}" does{} exist'.format(
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
