import os
import os.path
import sqlite3
import tempfile
import time

from filechanges import __version__
from filechanges.filechanges import (
    connectdb,
    createhashtable,
    debug,
    getbasefile,
    getfileext,
    getmoddate,
    md5short,
    tableexists,
)


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


def test_getfileext():
    assert getfileext("abc.txt") == "txt"
    assert getfileext("/tmp/abc.txt") == "txt"
    assert getfileext("/tmp/a/b/c/def.txt") == "txt"
    assert getfileext("abc") == ""
    assert getfileext(".abc") == ""
    assert getfileext(".abc.def") == "def"


def test_getmoddate():
    assert getmoddate("noname.txt") is None

    with tempfile.TemporaryFile(mode="w+t") as tf:
        debug(f"{tf.name=}")

        moddate = getmoddate(tf.name)
        debug(f"{type(moddate)=}, {moddate=}")

        assert isinstance(moddate, float)
        assert moddate > 0.0


def test_md5short():
    test_txt_file_name = os.path.join("test", "data", "file1.txt")
    test_md5_file_name = test_txt_file_name + ".md5"

    assert os.path.isfile(test_txt_file_name)
    assert os.path.isfile(test_md5_file_name)

    computed_md5 = md5short(test_txt_file_name)
    debug(f"computed MD5 = {computed_md5}")

    actual_md5 = open(test_md5_file_name, "rt").read().rstrip()
    debug(f"actual   MD5 = {actual_md5}")
    
    assert computed_md5 == actual_md5