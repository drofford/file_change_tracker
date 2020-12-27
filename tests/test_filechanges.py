import os
import os.path
import sqlite3

from filechanges import __version__
from filechanges.filechanges import connectdb, createhashtable, getbasefile, tableexists


def rm_db_file() -> str:
    db_filename = getbasefile() + ".db"
    if os.path.exists(db_filename):
        os.remove(db_filename)
    return db_filename


def test_version():
    assert __version__ == "0.1.0"


def test_getbasefile():
    assert getbasefile() == "filechanges"


def test_connectdb_new():
    f = rm_db_file()
    assert not os.path.exists(f)

    conn = connectdb()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    conn.commit()
    conn.close()

    return f


def test_connectdb_extant():
    f = test_connectdb_new()
    assert os.path.exists(f)

    conn = connectdb()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    conn.commit()
    conn.close()

    # assert False


def test_tableexists_false():
    test_connectdb_new()

    assert not tableexists("sparky")


def test_createhashtable():
    f = rm_db_file()

    assert createhashtable("sparky")
    assert not createhashtable("sparky")
