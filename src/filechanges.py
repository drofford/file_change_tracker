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
_logging_.basicConfig(format="%(asctime)s %(levelname)s : %(message)s",
    level=_logging_.DEBUG if os.getenv("DEBUG", "N") == "Y" else _logging_.INFO)

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
    f = inspect.currentframe()
    _logging_.info(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


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

"""Returns the name of the SQLite DB file"""
def getbasefile() -> str:
    trace("enter")
    return os.path.splitext(os.path.basename(__file__))[0]


class DbConnections:

    connections = dict()

    @classmethod
    def connect(cls, dbfilename=None, timeout=2):
        trace(f"enter: {dbfilename=}, {timeout=}")

        if (dbfilename := trim_to_none(dbfilename)) is None:
            debug(f"initial {dbfilename=}")
            dbfilename = getbasefile() + ".db"
            debug(f"updated {dbfilename=}, of type {type(dbfilename)}")

        conn = None

        if dbfilename in cls.connections:
            debug(f"reusing D/B connection for {dbfilename=}: {cls.connections[dbfilename]}")
            return cls.connections[dbfilename]

        try:
            cls.connections[dbfilename]  = sqlite3.connect(dbfilename, timeout=timeout)
            debug(f"created new D/B connection {dbfilename=}: {cls.connections[dbfilename]}")
            return cls.connections[dbfilename]

        except Exception as ex:
            fatal(f"Failed to establish a D/B connection for {dbfilename=}. Exception is: {ex}")

        return None




"""Connects to the SQLite DB"""
def connectdb():
    trace("enter")
    return DbConnections.connect(getbasefile())

def main():
    trace("enter")

    conn = connectdb()
    debug(f"{conn=}")



if __name__ == "__main__":
    main()
