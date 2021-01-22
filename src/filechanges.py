import inspect
import logging as _logging_
import os
import os.path
import sys
import sqlite3

from functools import lru_cache

#
# constants
#
TABLE_NAME = "status"

#
# global data
#
conn = None

#
# do some basic initial setup
#
_logging_.basicConfig(
    format="%(asctime)s %(levelname)s : %(message)s",
    level=_logging_.DEBUG if os.getenv("DEBUG", "N") == "Y" else _logging_.INFO,
)

#
# basic logging helpers
#
def trace(msg):
    f = inspect.currentframe()
    _logging_.debug(f"TRACE : {f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def debug(msg):
    f = inspect.currentframe()
    _logging_.debug(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def info(msg):
    # f = inspect.currentframe()
    _logging_.info(msg)


def error(msg):
    f = inspect.currentframe()
    _logging_.error(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def fatal(msg):
    f = inspect.currentframe()
    _logging_.error(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")
    exit(1)


def trim_to_none(inp_value):
    trace(f"{inp_value=}")

    out_value = None
    if inp_value is not None:
        inp_value = inp_value.strip()
        if len(inp_value) > 0:
            out_value = inp_value

    trace(f"{out_value=}")
    return out_value


#
# ============================================
# functions required by website's requirements
# ============================================
#


def corecursor(
    conn: sqlite3.Connection, query: str, args: list = None, hits: list = None
) -> bool:
    """Perform a query on the database table."""

    trace(f'query = "{query}"')

    result = False
    cursor = conn.cursor()
    try:
        if args is None:
            debug("invoking cursor.execute() without args")
            r = cursor.execute(query)
            debug(f"cursor.execute() returned {r}")
        else:
            debug("invoking cursor.execute() with {args=}")
            r = cursor.execute(query, args)
            debug(f"cursor.execute() returned {r}")
        rows = cursor.fetchall()
        numrows = len(list(rows))
        debug(f"{numrows=}")
        debug(f"before {len(hits) if hits is not None else 0}")
        if numrows > 0:
            for row in rows:
                debug(f"  {row=}")
                if hits is not None:
                    hits.append(list(row))
            result = True
        debug(f"after  {len(hits) if hits is not None else 0}")
    except sqlite3.OperationalError as err:
        error(str(err))  # fatal, maybe?
        result = None
    finally:
        cursor.close()

    debug(f"returning {result}")
    return result


def connectdb(dbfilename: str = None) -> sqlite3.Connection:
    if dbfilename is None:
        dbfilename = getbasefile()
    if not dbfilename.endswith(".db"):
        dbfilename += ".db"
    return _connectdb(dbfilename)


@lru_cache
def _connectdb(dbfilename: str) -> sqlite3.Connection:
    print(f'connecting to dbfilename "{dbfilename}"')
    try:
        con = sqlite3.connect(dbfilename, timeout=2)
        print(f"success: {con=}")
        return con

    except Exception as ex:
        print(f"fork it: the shirt has really hit the fan: {ex=}")
        exit(1)


def getbasefile() -> str:
    """Returns the name of the SQLite DB file"""
    trace("enter")
    return os.path.splitext(os.path.basename(__file__))[0]


def tableexists(dbfilename=None, tablename=TABLE_NAME):
    """Checks if a SQLite DB Table exists"""
    trace("enter")

    result = False

    try:
        conn = connectdb()

        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        debug(f"{query=}")

        args = (tablename,)
        debug(f"{args=}")

        result = corecursor(conn, query, args)

    except sqlite3.OperationalError as err:
        error(str(err))

    finally:
        conn.close()


def main():
    """Main function - does all of the control logic"""
    trace("enter")

    basename = getbasefile()
    conn = connectdb(basename)
    info(f'connected to database file "{basename}"')

    info(f"table \"{TABLE_NAME}\" {'exists' if tableexists() else 'does not exist'}")


if __name__ == "__main__":
    main()
