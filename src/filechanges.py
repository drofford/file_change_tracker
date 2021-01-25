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

global cfgfilename, dbfilename, tablename, flds, exts
cfgfilename = None
dbfilename = None
tablename = "status"
flds      = None
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

def connectdb() -> sqlite3.Connection:
    assert dbfilename is not None

    debug(f'calling _connectdb to connect to dbfilename "{dbfilename}"')
    conn = _connectdb(dbfilename)

    debug(f'_connectdb for dbfilename "{dbfilename}" returned {conn=}')
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


def createhashtable() -> bool:
    """Creates the named table if it does not exist"""
    trace(f"enter")
    cmd = f"CREATE TABLE {tablename} (id integer primary key, fname text, md5 text, moddate integer)"
    debug(f"command = {cmd}")

    result = runcmd(cmd)
    debug(f"runcmd returned {result}")

    debug(f"returning {result}")
    return result


def getbasefile() -> str:
    """Returns the name of the SQLite DB file"""
    trace("enter")
    return os.path.splitext(os.path.basename(__file__))[0]

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
        debug(f'reading config file "{cfgfilename}"')
        with open(cfgfilename, "rt") as cfg:
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

    assert cfgfilename is not None
    flds, exts = readconfig()

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


def tableexists():
    """Checks if a SQLite DB Table exists"""
    trace("enter")

    result = None

    try:
        conn = connectdb()

        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tablename}'"
        debug(f"{query=}")

        hits = list()
        r = corecursor(conn, query, hits=hits) #, args)
        debug(f"=====> {r}")
        debug(f"{hits}")
        if r and len(hits) > 0 and hits[0][0] == tablename:
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

    global cfgfilename, dbfilename

    basename = getbasefile()
    cfgfilename = basename + ".ini"
    dbfilename = basename + ".db"

    #
    # check that the main table exists, and if not create it
    #
    if tableexists():
        info(f"table \"{tablename}\" exists")
    else:
        info(f"table \"{tablename}\" does not exist and will be created")
        r = createhashtable()
        info(f"{r=}")

    #
    # read in and parse the config file
    #
    loadflds()
    info(f"configuration loaded from file \"{cfgfilename}\"")
    assert flds is not None
    assert exts is not None

if __name__ == "__main__":
    main()
