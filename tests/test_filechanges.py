import os
import os.path
import sqlite3

from filechanges import __version__
from filechanges.filechanges import getbasefile, connectdb


def test_version():
    assert __version__ == "0.1.0"


def test_getbasefile():
    assert getbasefile() == "filechanges"


def test_connectdb_new():
    db_filename = getbasefile() + ".db"
    if os.path.exists(db_filename):
        os.remove(db_filename)

    conn = connectdb()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    conn.commit()
    conn.close()

def test_connectdb_extant():
    test_connectdb_new()
    assert os.path.exists(getbasefile() + ".db")

    conn = connectdb()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    conn.commit()
    conn.close()
