#!/usr/bin/env python

from functools import lru_cache

import sqlite3


@lru_cache
def calc(x):
    print(f"calc: {x=}")
    return 100 * x


calc(1)
calc(2)
calc(3)
calc(4)
calc(1)
calc(1)
calc(10)

def getbasefile() -> str:
    """Returns the name of the SQLite DB file"""
    trace("enter")
    return os.path.splitext(os.path.basename(__file__))[0]


@lru_cache
def _connectdb(dbfilename: str) -> object:
    print(f"connecting to {dbfilename=}")
    try:
        con = sqlite3.connect(dbfilename, timeout=2)
        print(f"success: {con=}")
        return con

    except Exception as ex:
        print(f"shirt hit the fan: {ex=}")
        exit(1)

def connectdb(dbfilename=None):
    if dbfilename is None:
        dbfilename = getbasefile()
    if not dbfilename.endswith(".db"):
        dbfilename += ".db"
    return _connectdb(dbfilename)


c = connectdb("db01"); print(f"{c=}")
c = connectdb("db02"); print(f"{c=}")
c = connectdb("db03"); print(f"{c=}")
c = connectdb("db04"); print(f"{c=}")
c = connectdb("db01"); print(f"{c=}")
c = connectdb("db01"); print(f"{c=}")
c = connectdb("db10"); print(f"{c=}")
c = connectdb("db10"); print(f"{c=}")
