import hashlib
import inspect
import logging as _logging_
import os
import os.path
import pprint
import re
import sqlite3

from functools import lru_cache

#
# constants
#

#
# global data
#
conn = None

global cfg_file_name, db_file_name, table_name, flds, exts
cfg_file_name = None
db_file_name = None
table_name = "status"
flds = None
exts = None

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
    # _logging_.info(msg)
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


#
# ============================================
# functions required by website's requirements
# ============================================
#

def checkfilechanges(folder: str, excludes: list, ws: object) -> bool:
    """Checks for files changes"""
    trace(f"enter: {folder=}, {excludes=}")
    changed = False
    info("=" * 100)
    info(f'checking dir "{folder}"')
    need_to_commit = False
    for subdir, dirs, files in os.walk(folder):
        for fname in files:
            origin = os.path.join(subdir, fname)
            info("-" * 100)
            info(f'checking file "{origin}"')
            if not os.path.isfile(origin):
                info(f'skipping non-file "{origin}"')
            else:
                # Get file extension and check if it is not excluded
                ext = getfileext(origin)
                info(f'file extension is "{ext}"')

                if len(ext) == 0:
                    info(f'will process file "{origin}"')
                elif ext not in excludes:
                    info(f'will process file "{origin}" with extension "{ext}"')
                else:
                    debug(f'skipping file "{origin}" with excluded extension "{ext}"')
                    continue

                # check to see if the file already exists in the table - we do
                # this by computing the MD5 and looking it up in the index

                hits = list()
                file_in_table = is_file_in_table(origin, hits=hits)
                info(f"QQRXQ {origin=} {file_in_table=} {hits=}")

                cur_md5_val = md5short(origin)
                info(f"QQRXQ {origin=} {cur_md5_val=}")

                if not file_in_table:
                    r = inserthashtable(origin, cur_md5_val)
                    info(f"QQRXQ {origin=} {cur_md5_val=} inserthashtable returned {r=}")
                    need_to_commit = True

                # # Get the file’s md5 hash
                # hash = md5short(origin)
                # debug(f"        calculated file MD5 hash as {hash}")
                # add_to_report = False
                # debug(f"        checking if file already in hash table")
                # if not md5indb(origin):
                #     debug(f"            file not in hash table - will add")
                #
                #     # Get the file’s md5 hash
                #     hash = md5short(origin)
                #     debug(f"        calculated file MD5 hash as {hash}")
                #
                #     r = inserthashtable(origin, hash)
                #     debug(f"            file not in hash table - added ({r})")
                #     add_to_report = True
                # elif not haschanged(origin, hash):
                #     debug(
                #         f"            file in hash table and MD5 unchanged - not update"
                #     )
                # else:
                #     debug(
                #         f"            file in hash table and MD5 *has* changed - will update"
                #     )
                #     r = updatehashtable(origin, hash)
                #     debug(
                #         f"            file in hash table and MD5 *has* changed - updated ({r})"
                #     )
                #     add_to_report = True
                #     # If the file has changed, add it to the Excel report
                #     debug(f"file in hash table and MD5 *has* changed - updated")
                # if add_to_report:
                #     info(f'Will add file "{origin}" to report')
                #     changed = True
        # if need_to_commit:
        #     hits = list()
        #     r = runcmd("commit", hits=hits)
        #     info(f"{need_to_commit=}, {r=}, {hits=}")
    debug("=" * 100)
    debug(f"returning {changed=}")
    return changed


def connectdb() -> sqlite3.Connection:
    assert db_file_name is not None

    debug(f'calling _connectdb to connect to dbfilename "{db_file_name}"')
    conn = _connectdb(db_file_name)

    debug(f'_connectdb for dbfilename "{db_file_name}" returned {conn=}')
    return conn


# @lru_cache
def _connectdb(dbfilename: str) -> sqlite3.Connection:
    debug(f'connecting to dbfilename "{dbfilename}"')
    try:
        conn = sqlite3.connect(dbfilename, timeout=2)
        debug(f"success: {conn=}")
        return conn

    except Exception as ex:
        fatal(f"fork it: the shirt has really hit the fan: {ex=}")


def corecursor(
    conn: sqlite3.Connection, query: str, args: list = None, hits: list = None
) -> bool:
    """Perform a query on the database table."""

    trace(f'query = "{query}"')

    result = None
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
        if numrows == 0:
            result = False
        else:
            result = True
            if hits is not None:
                for row in rows:
                    debug(f"  {row=}")
                    hits.append(list(row))
        debug(f"after  {len(hits) if hits is not None else 0}")
    except sqlite3.OperationalError as err:
        error(str(err))  # fatal, maybe?
    finally:
        cursor.close()

    debug(f"returning {result}")
    return result


def createhashtable() -> None:
    """Creates the named table if it does not exist"""
    trace(f"enter")
    cmd = f"CREATE TABLE {table_name} (id integer primary key, fname text, md5 text, moddate integer)"
    debug(f"command = {cmd}")

    result = runcmd(cmd)
    debug(f"runcmd returned {result}")
    if not result:
        fatal(f'Failed to create table "{table_name}"')

    info(f'created table "{table_name}"')


def createhashtableidx() -> None:
    """Creates the SQLite DB Table Indices"""
    trace(f"enter")

    def create_index(column_name):
        trace(f"enter: {column_name=}")

        index_name = f"idx_{table_name}_{column_name}"
        cmd = f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({column_name})"
        debug(f"command = {cmd}")

        result = runcmd(cmd)
        debug(f"runcmd returned {result}")
        if not result:
            fatal(
                f'Failed to create index "{index_name}" on column "{column_name}" of table "{table_name}"'
            )

        info(
            f'created index "{index_name}" on column "{column_name}" of table "{table_name}"'
        )

    create_index("fname")
    create_index("md5")


def getbasefile() -> str:
    """Returns the name of the SQLite DB file"""
    trace("enter")
    return os.path.splitext(os.path.basename(__file__))[0]

def getfileext(fname: str) -> str:
    """Get the file name extension.

    Extension does NOT include the dot. Dotfiles like ".bashrc" are handled as
    expected, with the filename = ".bashrc" and the extension as "".
    """
    trace(f"enter: fname = {fname}")
    return os.path.splitext(os.path.basename(fname))[1]

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

def inserthashtable(fname:str, md5:str) -> bool:
    """Insert into the SQLite File Table"""

    cmd = "BEGIN TRANSACTION"
    info("QQRXQ beginning transaction")
    result = runcmd(cmd)
    info(f"QQRXQ runcmd BEGIN TRANSACTION returned {result=}")

    cmd = f"INSERT INTO {table_name} (fname, md5, moddate) VALUES (?, ?, ?)"
    args = (fname, md5, int(getmoddate(fname)))

    info(f"QQRXQ running SQL INSERT command for {md5=} {fname=}")
    result = runcmd(cmd, args)
    info(f"QQRXQ runcmd INSERT returned {result=}")

    cmd = "END TRANSACTION"
    info("QQRXQ ending transaction")
    result = runcmd(cmd)
    info(f"QQRXQ runcmd END TRANSACTION returned {result=}")

    return result

def is_file_in_table(fname: str, hits: list=None) -> bool:
    """Checks if md5 hash tag exists in the SQLite DB"""

    conn = connectdb()

    result = False
    try:
        query = f"SELECT fname, md5 FROM {table_name} WHERE fname = ?"
        debug(f"{query=}")
        args = (fname,)
        debug(f"{args=}")
        r = corecursor(conn, query, args, hits)
        info(f"{r=}, {hits=}")
        if r and len(hits) == 1:
            result = True
    except sqlite3.OperationalError as err:
        error(str(err))
    finally:
        conn.close()

    debug(f"{result=}")
    return result

def loadflds() -> tuple:
    trace(f"entry")

    global flds, exts

    def readconfig() -> tuple:
        trace(f"entry")

        dirs_and_exts_map = dict()
        dirs_map = dict()
        exts_map = dict()

        def process_dir(dir_path: str) -> str:
            trace(f"entry")

            if "\\" in dir_path and ":" in dir_path:
                debug(f'       processing windows-style path "{dir_path}"')
                style = "windows"
            else:
                debug(f'       processing posix-style path "{dir_path}"')
                style = "posix"

            if dir_path not in dirs_and_exts_map:
                dirs_and_exts_map[dir_path] = {"count": 1, "exts": {}, "style": style}
            else:
                dirs_and_exts_map[dir_path]["count"] += 1
            dirs_map[dir_path] = {}

            return style

        def process_exts(dir_path: str, exts: str) -> None:
            trace(f"entry")

            def process_ext(ext: str) -> None:
                trace(f"entry")
                if ext.startswith("."):
                    ext = ext[1:]
                dirs_and_exts_map[dir_path]["exts"][ext] = {"count": 0}

            debug(f"      processing {exts=}")
            for ext in exts.split(","):
                ext = ext.strip()
                debug(f"          processing {ext=}")
                process_ext(ext)
                exts_map[ext] = None

        debug("=" * 78)
        debug(f'reading config file "{cfg_file_name}"')
        with open(cfg_file_name, "rt") as cfg:
            all_styles = None

            for line_num, line_buf in enumerate(cfg):
                line_buf = line_buf.strip()
                debug("-" * 78)
                debug(f"[{line_num:04}] : {line_buf}")

                if len(line_buf) == 0 or line_buf.startswith("#"):
                    debug("    skipping blank or comment line")
                    continue

                parts = line_buf.split("|")
                debug(f"    {len(parts)=} : {parts=}")

                if len(parts) == 1:
                    debug(f'    process dir "{parts[0]}"')
                    this_style = process_dir(parts[0])
                elif len(parts) == 2:
                    debug(f'    process dir  "{parts[0]}"')
                    this_style = process_dir(parts[0])
                    debug(f'    process exts "{parts[1]}"')
                    process_exts(parts[0], parts[1])
                else:
                    msg = f"error in line {line_num}: too many '|' characters ({len(parts)-1})"
                    error(msg)
                    raise ValueError(msg)
                if all_styles is None:
                    all_styles = this_style
                elif this_style != all_styles:
                    msg = f'error in line {line_num}: {this_style} style dir path "{parts[0]}" cannot be mixed with {all_styles} dir paths'
                    error(msg)
                    raise ValueError(msg)

        debug("=" * 78)
        debug(f"dir to ext map = \n{pprint.pformat(dirs_and_exts_map)}")
        debug("-" * 78)
        debug(f"dirs map = \n{pprint.pformat(dirs_map)}")
        debug("-" * 78)
        debug(f"exts map = \n{pprint.pformat(exts_map)}")
        debug("=" * 78)

        # return dirs_and_exts_map
        # result sorted(list(dirs_map.keys())), sorted(list(exts_map.keys()))
        a = sorted(list(dirs_map.keys()))
        b = sorted(list(exts_map.keys()))
        return a, b

    assert cfg_file_name is not None
    flds, exts = readconfig()

def md5short(fname):
    """Get md5 file hash"""

    data = open(fname, "rb").read()
    # debug(f'read {len(data)} {type(data)} from "{fname}"')

    md = hashlib.new("md5")
    md.update(data)
    md5val = md.hexdigest()

    # debug(f'MD5 for "{fname=}" is {md5val}')

    return md5val

def runcmd(cmd: str, args: list = None, hits: list = None) -> bool:
    """Run a specific command on the SQLite DB"""
    trace(f"entry: {cmd=}")

    result = None

    conn = connectdb()
    debug(f"{conn=}, {type(conn)=}")
    # debug(f"{cmd=}")
    # debug(f"{args=}")
    # debug(f"{dir(conn)=}")

    cursor = conn.cursor()
    debug(f"cursor = {cursor}")

    try:
        if args is None:
            debug("invoking cursor.execute() without args")
            r = cursor.execute(cmd)
        else:
            debug("invoking cursor.execute() with args")
            r = cursor.execute(cmd, args)
        debug(f"cursor.execute() returned {r}")

        rows = cursor.fetchall()
        numrows = len(list(rows))
        debug(f"{numrows=}")
        if numrows > 0:
            if hits is not None:
                for row in rows:
                    debug(f"  {row=}")
                    hits.append(list(row))
        result = True

    except sqlite3.IntegrityError as err:
        error(str(err))
    except sqlite3.OperationalError as err:
        if len(re.findall(r"table\s+(\S+)\s+already\s+exists", str(err))) > 0:
            debug(str(err))
        else:
            error(str(err))
    finally:
        cursor.close()

    conn.close()

    debug(f"returning {result}")
    return result


def runfilechanges(ws: object = None) -> bool:
    trace(f"enter: {ws=}")

    #
    # read in and parse the config file
    #
    debug("getting list of dirs to be scanned and extensions to be ignored")

    loadflds()
    assert flds is not None
    assert exts is not None

    info(f'the configuration was loaded from file "{cfg_file_name}"')
    info(f"discovered {len(flds)} dirs and {len(exts)} extensions")

    if len(flds) == 0:
        info(f"no directories to be scanned")
        return False

    changed = False
    wid = len(str(len(flds)-1))
    for i, fld in enumerate(flds, start=1):
        # Invoke the function that checks each folder for file changes
        info(f"Processing directory {i:{wid}} of {len(flds)}: \"{fld}\"")

        r = checkfilechanges(fld, exts, ws)
        info(f"checkfilechanges(...) returned {r=}")

        if r:
            changed = r

    debug(f"returning {changed=}")
    return changed


def tableexists():
    """Checks if a SQLite DB Table exists"""
    trace("enter")

    result = None

    try:
        conn = connectdb()

        query = (
            # f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            f"SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        )
        debug(f"{query=}")

        args = (table_name,)

        hits = list()
        r = corecursor(conn, query, args, hits=hits)
        debug(f"=====> {r}")
        debug(f"{hits}")
        if r and len(hits) > 0 and hits[0][0] == table_name:
            result = True
        else:
            result = False

    except sqlite3.OperationalError as err:
        error(str(err))

    finally:
        conn.close()

    debug(f"tableexists() returning {result}")
    return result


def main():
    """Main function - does all of the control logic"""
    trace("enter")

    global cfg_file_name, db_file_name

    basename = getbasefile()
    cfg_file_name = basename + ".ini"
    db_file_name = basename + ".db"

    #
    # check that the main table exists, and if not create it
    #
    if tableexists():
        info(f'table "{table_name}" exists')
    else:
        info(f'table "{table_name}" does not exist and will be created')
        createhashtable()
        createhashtableidx()

    runfilechanges()


if __name__ == "__main__":
    main()
