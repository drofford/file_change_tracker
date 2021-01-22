import inspect
import logging as _logging_
import os
import os.path
import sys
import sqlite3


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
# helper class for D/B connection pool
#
class DbConnections:

    connections = dict()

    @classmethod
    def connect(cls, dbfilename, timeout=2):
        trace(f"enter: {dbfilename=}, {timeout=}")

        if not dbfilename.endswith(".db"):
            dbfilename += ".db"
            debug(f"updated {dbfilename=}")

        if dbfilename in cls.connections:
            debug(
                f"reusing D/B connection for {dbfilename=}: {cls.connections[dbfilename]}"
            )
        else:
            debug(f"will create new D/B connection for {dbfilename=}")
            try:
                cls.connections[dbfilename] = sqlite3.connect(
                    dbfilename, timeout=timeout
                )
                debug(
                    f"created new D/B connection {dbfilename=}: {cls.connections[dbfilename]}"
                )

            except Exception as ex:
                fatal(
                    f"Failed to establish a D/B connection for {dbfilename=}. Exception is: {ex}"
                )

        return cls.connections[dbfilename]

    @classmethod
    def close(cls, dbfilename):
        trace(f"enter: {dbfilename=}, {timeout=}")
        if not dbfilename.endswith(".db"):
            dbfilename += ".db"
            debug(f"updated {dbfilename=}")

        if dbfilename in cls.connections:
            debug(
                f"closing D/B connection for {dbfilename=}: {cls.connections[dbfilename]}"
            )
            cls.connections[dbfilename].close()
            del cls.connections[dbfilename]

    @classmethod
    def close_all(cls):
        for x in cls.connections.keys():
            cls.close(x)
            cls.connections = dict()
        debug(f"all D/B connections closed")


#
# ============================================
# functions required by website's requirements
# ============================================
#


def getbasefile() -> str:
    """Returns the name of the SQLite DB file"""
    trace("enter")
    return os.path.splitext(os.path.basename(__file__))[0]


def connectdb(dbfilename=None):
    """Connects to the SQLite DB"""
    trace("enter")

    debug(f"initial {dbfilename=}")
    if (dbfilename := trim_to_none(dbfilename)) is None:
        dbfilename = getbasefile()
        debug(f"updated {dbfilename=}")

    return DbConnections.connect(dbfilename)


def tableexists(dbfilename=None, tablename=TABLE_NAME):
    """Checks if a SQLite DB Table exists"""
    trace("enter")

    result = False

    try:
        conn = connectdb()

        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        debug(f"{query=}")

        args = (table,)
        debug(f"{args=}")
        # result = corecursor(conn, query, args)

    except sqlite3.OperationalError as err:
        error(str(err))

    finally:
        conn.close()


def main():
    """Main function - does all of the control logic"""
    trace("enter")

    basename = getbasefile()
    conn = connectdb(basename)
    info(f"connected to database file {basename}")


if __name__ == "__main__":
    main()
