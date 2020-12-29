import os
import os.path
import sqlite3
import tempfile
import time

from filechanges import __version__
from filechanges.filechanges import (
    FILE_TABLE_NAME,
    connectdb,
    createhashtable,
    createhashtableidx,
    debug,
    getbasefile,
    getfileext,
    getmoddate,
    haschanged,
    inserthashtable,
    md5short,
    runcmd,
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
    # conn.commit()
    conn.close()

    return f


def test_connectdb_extant():
    f = test_connectdb_new()
    assert os.path.exists(f)

    conn = connectdb()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    # conn.commit()
    conn.close()


def test_tableexists_false():
    test_connectdb_new()

    assert not tableexists(FILE_TABLE_NAME)


def test_createhashtable():
    f = rm_db_file()

    assert createhashtable(FILE_TABLE_NAME)
    assert not createhashtable(FILE_TABLE_NAME)


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


def test_haschanged():
    test_txt_file_name = os.path.join("test", "data", "file1.txt")
    computed_md5 = md5short(test_txt_file_name)

    assert not haschanged(test_txt_file_name, computed_md5)

    adjusted_md5 = "x" + computed_md5[1:]
    assert haschanged(test_txt_file_name, adjusted_md5)


def test_createhashtableidx():
    f = rm_db_file()
    assert createhashtable(FILE_TABLE_NAME)
    assert createhashtableidx(FILE_TABLE_NAME)


def test_runcmd_no_conn():
    f = rm_db_file()

    cmd = "CREATE TABLE dummy (id integer primary key, fname text, lname text)"
    debug(f"command = {cmd}")

    r = runcmd(cmd)
    debug(f"runcmd returned {r}")
    assert r

    r = runcmd(cmd)
    debug(f"runcmd returned {r}")
    assert not r


def test_inserthashtable_one():
    f = rm_db_file()
    assert createhashtable(FILE_TABLE_NAME)
    assert createhashtableidx(FILE_TABLE_NAME)

    test_txt_file_name = os.path.join("test", "data", "file1.txt")
    assert os.path.isfile(test_txt_file_name)

    computed_md5 = md5short(test_txt_file_name)
    debug(f"computed MD5 = {computed_md5}")

    r = inserthashtable(test_txt_file_name, computed_md5)
    debug(f"1st runcmd returned {r}")
    assert r

    r = inserthashtable(test_txt_file_name, computed_md5)
    debug(f"2nd runcmd returned {r}")
    assert not r

    

def test_inserthashtable_two():
    f = rm_db_file()
    assert createhashtable(FILE_TABLE_NAME)
    assert createhashtableidx(FILE_TABLE_NAME)

    test_txt_file1_name = os.path.join("test", "data", "file1.txt")
    assert os.path.isfile(test_txt_file1_name)

    test_txt_file1_computed_md5 = md5short(test_txt_file1_name)
    debug(f"computed MD5 = {test_txt_file1_computed_md5}")

    r = inserthashtable(test_txt_file1_name, test_txt_file1_computed_md5)
    debug(f"1st runcmd returned {r}")
    assert r

    
    test_txt_file2_name = os.path.join("test", "data", "file2.txt")
    assert os.path.isfile(test_txt_file2_name)

    test_txt_file2_computed_md5 = md5short(test_txt_file2_name)
    debug(f"computed MD5 = {test_txt_file2_computed_md5}")

    r = inserthashtable(test_txt_file2_name, test_txt_file2_computed_md5)
    debug(f"2nd runcmd returned {r}")
    assert r
