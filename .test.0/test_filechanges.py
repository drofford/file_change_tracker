import os
import os.path
import shutil
import sqlite3
import tempfile
import time
import traceback

from filechanges import __version__
from filechanges.filechanges import (
    FILE_TABLE_NAME,
    connectdb,
    createhashtable,
    createhashtableidx,
    debug,
    error,
    getbasefile,
    getfileext,
    getmoddate,
    haschanged,
    info,
    inserthashtable,
    loadflds,
    logger,
    md5indb,
    md5short,
    readconfig,
    runcmd,
    runfilechanges,
    setuphashtable,
    tableexists,
    updatehashtable,
)


def rm_db_file() -> str:
    db_filename = getbasefile() + ".db"
    if os.path.exists(db_filename):
        os.remove(db_filename)
    return db_filename


def __test_version() -> None:
    assert __version__ == "0.1.0"


def __test_getbasefile() -> None:
    assert getbasefile() == "filechanges"


def __test_connectdb_new() -> None:
    f = rm_db_file()
    assert not os.path.exists(f)

    conn = connectdb()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    # conn.commit()
    conn.close()

    return f


def __test_connectdb_extant() -> None:
    f = test_connectdb_new()
    assert os.path.exists(f)

    conn = connectdb()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    # conn.commit()
    conn.close()


def __test_tableexists_false() -> None:
    test_connectdb_new()

    assert not tableexists(FILE_TABLE_NAME)


def __test_createhashtable() -> None:
    f = rm_db_file()

    assert createhashtable(FILE_TABLE_NAME)
    assert not createhashtable(FILE_TABLE_NAME)


def __test_getfileext() -> None:
    assert getfileext("abc.txt") == ".txt"
    assert getfileext("/tmp/abc.txt") == ".txt"
    assert getfileext("/tmp/a/b/c/def.txt") == ".txt"
    assert getfileext("abc") == ""
    assert getfileext(".abc") == ""
    assert getfileext(".abc.def") == ".def"


def __test_getmoddate() -> None:
    assert getmoddate("noname.txt") is None

    with tempfile.TemporaryFile(mode="w+t") as tf:
        debug(f"{tf.name=}")

        moddate = getmoddate(tf.name)
        debug(f"{type(moddate)=}, {moddate=}")

        assert isinstance(moddate, float)
        assert moddate > 0.0


def __test_md5short() -> None:
    test_txt_file_name = os.path.join("test", "data", "file1.txt")
    test_md5_file_name = test_txt_file_name + ".md5"

    assert os.path.isfile(test_txt_file_name)
    assert os.path.isfile(test_md5_file_name)

    computed_md5 = md5short(test_txt_file_name)
    debug(f"computed MD5 = {computed_md5}")

    actual_md5 = open(test_md5_file_name, "rt").read().rstrip()
    debug(f"actual   MD5 = {actual_md5}")

    assert computed_md5 == actual_md5


def __test_haschanged() -> None:
    test_txt_file_name = os.path.join("test", "data", "file1.txt")
    computed_md5 = md5short(test_txt_file_name)

    assert not haschanged(test_txt_file_name, computed_md5)

    adjusted_md5 = "x" + computed_md5[1:]
    assert haschanged(test_txt_file_name, adjusted_md5)


def __test_createhashtableidx() -> None:
    f = rm_db_file()
    assert createhashtable(FILE_TABLE_NAME)
    assert createhashtableidx(FILE_TABLE_NAME)


def __test_runcmd_no_conn() -> None:
    f = rm_db_file()

    cmd = "CREATE TABLE dummy (id integer primary key, fname text, lname text)"
    debug(f"command = {cmd}")

    r = runcmd(cmd)
    debug(f"runcmd returned {r}")
    assert r

    r = runcmd(cmd)
    debug(f"runcmd returned {r}")
    assert not r


def __test_inserthashtable_one() -> None:
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


def __test_inserthashtable_two() -> None:
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


def __test_updatehashtable() -> None:
    f = rm_db_file()
    assert createhashtable(FILE_TABLE_NAME)
    assert createhashtableidx(FILE_TABLE_NAME)

    test_txt_file1_name = os.path.join("test", "data", "file1.txt")
    assert os.path.isfile(test_txt_file1_name)

    test_txt_file1_computed_md5 = md5short(test_txt_file1_name)
    debug(f"computed MD5 = {test_txt_file1_computed_md5}")

    r = inserthashtable(test_txt_file1_name, test_txt_file1_computed_md5)
    debug(f"1st runcmd [inserthashtable] returned {r}")
    assert r

    r = updatehashtable(test_txt_file1_name, "XYZMD5")
    debug(f"2nd runcmd [updatehashtable] returned {r}")
    assert r

    # assert False


def __test_setuphashtable() -> None:
    f = rm_db_file()

    test_txt_file1_name = os.path.join("test", "data", "file1.txt")
    assert os.path.isfile(test_txt_file1_name)

    test_txt_file1_computed_md5 = md5short(test_txt_file1_name)
    debug(f"computed MD5 = {test_txt_file1_computed_md5}")

    r = setuphashtable(test_txt_file1_name, test_txt_file1_computed_md5, table="sparky")
    debug(f"setuphashtable() returned {r}")
    assert r


def __test_md5indb() -> None:
    f = rm_db_file()
    assert createhashtable(FILE_TABLE_NAME)
    assert createhashtableidx(FILE_TABLE_NAME)

    test_txt_file1_name = os.path.join("test", "data", "file1.txt")
    assert os.path.isfile(test_txt_file1_name)

    test_txt_file1_computed_md5 = md5short(test_txt_file1_name)
    debug(f"computed MD5 = {test_txt_file1_computed_md5}")

    if md5_from_db := md5indb(test_txt_file1_name) is None:
        debug(f"(a) file \"{test_txt_file1_name}\" with computed MD5 \"{test_txt_file1_computed_md5}\" not found in database")
    else:
        debug(f"(a) file \"{test_txt_file1_name}\" with computed MD5 \"{test_txt_file1_computed_md5}\" found in database with D/B MD5 \"{md5_from_db}\"")
    assert md5_from_db is None

    r = inserthashtable(test_txt_file1_name, test_txt_file1_computed_md5)
    debug(f"inserthashtable returned {r}")
    assert r

    if md5_from_db := md5indb(test_txt_file1_name) is None:
        debug(f"(b) file \"{test_txt_file1_name}\" with computed MD5 \"{test_txt_file1_computed_md5}\" not found in database")
    else:
        debug(f"(b) file \"{test_txt_file1_name}\" with computed MD5 \"{test_txt_file1_computed_md5}\" found in database with D/B MD5 \"{md5_from_db}\"")
    assert md5_from_db is None

    assert False

def __test_loadflds_nsf() -> None:
    debug(f"trying to read config from non-existent implicit config file")

    cfg_file_name = getbasefile() + ".ini"
    if os.path.exists(cfg_file_name):
        os.remove(cfg_file_name)

    try:
        result = loadflds()
        debug(f"loadflds() returned {result}")
        assert False
    except ValueError as ex:
        pass


def create_default_config_file(ini_base_name: str = "example") -> tuple:
    src_cfg_file_name = os.path.join("test", "data", f"{ini_base_name}.ini")
    dst_cfg_file_name = getbasefile() + ".ini"
    debug(f'copying "{src_cfg_file_name}" to "{dst_cfg_file_name}"')

    r = shutil.copyfile(src_cfg_file_name, dst_cfg_file_name)
    debug(f"shutil.copyfile returned {r}")
    if not os.path.exists(dst_cfg_file_name):
        raise ValueError(
            f'failed to copy "{src_cfg_file_name}" to "{dst_cfg_file_name}"'
        )

    return src_cfg_file_name, dst_cfg_file_name


def __test_loadflds_sf() -> None:
    debug(f"trying to read config from extant implicit config file")

    src_cfg_file_name, dst_cfg_file_name = create_default_config_file()

    result = loadflds()
    debug(f"loadflds() returned {type(result)=}")
    debug(f"loadflds() returned {result=}")
    assert result is not None
    assert isinstance(result, tuple)
    os.remove(dst_cfg_file_name)

    # assert False


def __test_readconfig_example() -> None:
    filename = os.path.join("test", "data", "example.ini")
    debug(f'reading config from explicit config file "{filename}"')

    result = readconfig(filename)
    assert result is not None
    assert isinstance(result, tuple)

    # assert False


def __test_readconfig_posix() -> None:
    filename = os.path.join("test", "data", "posix_style.ini")
    debug(f'reading config from explicit config file "{filename}"')

    result = readconfig(filename)
    assert result is not None
    assert isinstance(result, tuple)

    # assert False


def __test_readconfig_windows() -> None:
    filename = os.path.join("test", "data", "windows_style.ini")
    debug(f'reading config from explicit config file "{filename}"')

    result = readconfig(filename)
    assert result is not None
    assert isinstance(result, tuple)

    # assert False


def __test_readconfig_mixed_p() -> None:
    filename = os.path.join("test", "data", "mixed_styles_p.ini")
    debug(f'reading config from explicit config file "{filename}"')

    result = False
    try:
        readconfig(filename)
    except ValueError as ex:
        result = True
    assert result

    # assert False


def __test_readconfig_mixed_w() -> None:
    filename = os.path.join("test", "data", "mixed_styles_w.ini")
    debug(f'reading config from explicit config file "{filename}"')

    result = False
    try:
        readconfig(filename)
    except ValueError as ex:
        result = True
    assert result

    # assert False


def __test_runfilechanges() -> None:
    f = rm_db_file()
    assert createhashtable(FILE_TABLE_NAME)
    assert createhashtableidx(FILE_TABLE_NAME)

    if os.sys.platform.startswith("win"):
        style = "windows"
    else:
        style = "posix"

    debug("="*100)
    debug("="*100)
    debug("="*100)
    debug(f"Should add everything / update nothing")
    debug("="*100)
    debug("="*100)
    debug("="*100)
    src_cfg_file_name, dst_cfg_file_name = create_default_config_file(style + "_style")
    r = runfilechanges("WTF")
    debug(f"{r=}")
    # assert r

    debug("="*100)
    debug("="*100)
    debug("="*100)
    debug(f"Should add nothing / update nothing")
    debug("="*100)
    debug("="*100)
    debug("="*100)
    src_cfg_file_name, dst_cfg_file_name = create_default_config_file(style + "_style")
    r = runfilechanges("WTF")
    debug(f"{r=}")
    # assert not r

    debug("="*100)
    debug("="*100)
    debug("="*100)
    debug(f"Should add volatile_file.txt / update nothing")
    debug("="*100)
    debug("="*100)
    debug("="*100)
    # create a temp file in the test/files/dynamic subdir
    outfile = os.path.join("test", "files", "dynamic", "volatile_file.txt")

    with open(outfile, "wt") as fh:
        fh.write(str(time.time()))
        debug(f"created file \"{fh.name}\"")

    src_cfg_file_name, dst_cfg_file_name = create_default_config_file(style + "_style")
    r = runfilechanges("WTF")
    # assert r
    debug(f"{r=}")

    debug("="*100)
    debug("="*100)
    debug("="*100)
    debug(f"Should add nothing / update nothing")
    debug("="*100)
    debug("="*100)
    debug("="*100)
    src_cfg_file_name, dst_cfg_file_name = create_default_config_file(style + "_style")
    r = runfilechanges("WTF")
    debug(f"{r=}")
    # assert not r

    with open(outfile, "wt") as fh:
        fh.write(str(time.time()))
        debug(f"recreated file \"{fh.name}\"")

    debug("="*100)
    debug("="*100)
    debug("="*100)
    debug(f"Should add nothing / update volatile_file.txt")
    debug("="*100)
    debug("="*100)
    debug("="*100)
    src_cfg_file_name, dst_cfg_file_name = create_default_config_file(style + "_style")
    r = runfilechanges("WTF")
    debug(f"{r=}")
    # assert r

    debug("="*100)
    debug("="*100)
    debug("="*100)
        
    assert False