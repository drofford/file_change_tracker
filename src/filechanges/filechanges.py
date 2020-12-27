import daiquiri
import hashlib
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
    """Checks to see if a table exists"""
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
    """Creates the named table if it does not exist"""
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


def getfileext(fname: str) -> str:
    """Get the file name extension.

    Extension does NOT include the dot. Dotfiles like ".bashrc" are handled as
    expected, with the filename = ".bashrc" and the extension as "".
    """
    return os.path.splitext(os.path.basename(fname))[1][1:]


def haschanged(fname: str, md5: str) -> bool:
    """Checks if a file has changed"""
    result = False

    calc_md5 = md5short(fname)
    debug(f'calculated MD5 for "{fname}" is {calc_md5}')
    debug(f'provided   MD5 for "{fname}" is {md5}')
    if md5 != calc_md5:
        debug(f'file "{fname}" has changed')
        result = True
    else:
        debug(f'file "{fname}" has not changed')

    # Call md5indb ...
    # if the file’s MD5 hash has changed...
    # Update the hash table ...
    # else:
    # Setup the hash table ...
    return result


def getmoddate(fname: str) -> object:
    """Get file modified date"""
    try:
        debug(f"{fname=}")
        mtime = os.path.getmtime(fname)
        debug(f"{type(mtime)=}, {mtime=}")
        return mtime
    except FileNotFoundError as ex:
        debug(f'no such file: "{fname}"')

    except Exception as ex:
        error(f'caught exception of type {type(ex)}: "{ex}", program abending')
        exit(1)

    return None


def md5short(fname):
    """Get md5 file hash tag"""

    data = open(fname, "rb").read()
    debug(f'read {len(data)} {type(data)} from "{fname}"')

    md = hashlib.new("md5")
    md.update(data)
    md5val = md.hexdigest()

    debug(f'MD5 for "{fname=}" is {md5val}')

    return md5val
