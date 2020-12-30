import daiquiri
import hashlib
import inspect
import logging as LOG
import os
import re
import sys
import sqlite3

#
# set up logging
#
level = LOG.DEBUG if os.getenv("DEBUG", "") == "Y" else LOG.INFO
LOG.basicConfig(format="%(asctime)s %(levelname)s : %(message)s", level=level)
logger = LOG.getLogger()

#
# constants
#
FILE_TABLE_NAME = "files"


def debug(msg):
    f = inspect.currentframe()
    logger.debug(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def info(msg):
    f = inspect.currentframe()
    logger.info(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def error(msg):
    f = inspect.currentframe()
    logger.error(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def fatal(msg):
    f = inspect.currentframe()
    logger.error(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")
    exit(1)


def getbasefile():
    """Returns the name of the SQLite DB file"""
    return os.path.splitext(os.path.basename(__file__))[0]


def connectdb() -> sqlite3.Connection:
    """Connects to the SQLite DB"""
    try:
        dbfile = getbasefile() + ".db"
        debug('DB file name is "{}"'.format(dbfile))
        conn = sqlite3.connect(dbfile, timeout=2)
        debug("connected w/ conn = {}".format(conn))
        return conn
    except Exception as ex:
        error("exception {} caught".format(ex))
        sys.stderr.flush()
        exit(1)


def corecursor(conn: sqlite3.Connection, query: str, args: list = None) -> bool:
    debug(f"query = \"{query}\"")
    
    result = False
    cursor = conn.cursor()
    try:
        if args is None:
            debug("invoking cursor.execute() without args")
            r = cursor.execute(query)
            debug(f"cursor.execute() returned {r}")
        else:
            debug("invoking cursor.execute() with args")
            r = cursor.execute(query, args)
            debug(f"cursor.execute() returned {r}")
        rows = cursor.fetchall()
        numrows = len(list(rows))
        debug(f"{numrows=}")
        if numrows > 0:
            for row in rows:
                debug(f"  {row=}")
            result = True
    except sqlite3.OperationalError as err:
        error(str(err))
    finally:
        cursor.close()

    debug(f"returning {result}")
    return result


def tableexists(table: str = FILE_TABLE_NAME) -> bool:
    """Checks to see if a table exists"""
    result = False
    conn = connectdb()
    if conn is not None:
        try:
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
            args = (table,)
            result = corecursor(conn, query, args)
        except sqlite3.OperationalError as err:
            error(str(err))
        finally:
            conn.close()

    return result


def createhashtable(table: str = FILE_TABLE_NAME) -> bool:
    """Creates the named table if it does not exist"""

    cmd = f"CREATE TABLE {table} (id integer primary key, fname text, md5 text, moddate integer)"
    debug(f"command = {cmd}")

    result = runcmd(cmd)
    debug(f"runcmd returned {result}")

    debug(f"returning {result}")
    return result


def createhashtableidx(table: str = FILE_TABLE_NAME) -> bool:
    """Creates a SQLite DB Table Index"""

    cmd = f"CREATE UNIQUE INDEX idx_{table} ON {table} (fname)"
    debug(f"command = {cmd}")

    result = runcmd(cmd)
    debug(f"runcmd returned {result}")

    debug(f"returning {result}")
    return result


def getfileext(fname: str) -> str:
    """Get the file name extension.

    Extension does NOT include the dot. Dotfiles like ".bashrc" are handled as
    expected, with the filename = ".bashrc" and the extension as "".
    """
    return os.path.splitext(os.path.basename(fname))[1][1:]


def haschanged(fname: str, md5: str) -> bool:
    """Checks if a file has changed"""
    result = False

    calc_md5 = md5short(fname)
    debug(f'calculated MD5 for "{fname}" is {calc_md5}')
    debug(f'provided   MD5 for "{fname}" is {md5}')
    if md5 != calc_md5:
        debug(f'file "{fname}" has changed')
        result = True
    else:
        debug(f'file "{fname}" has not changed')

    # Call md5indb ...
    # if the file’s MD5 hash has changed...
    # Update the hash table ...
    # else:
    # Setup the hash table ...
    return result


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


def md5short(fname):
    """Get md5 file hash tag"""

    data = open(fname, "rb").read()
    debug(f'read {len(data)} {type(data)} from "{fname}"')

    md = hashlib.new("md5")
    md.update(data)
    md5val = md.hexdigest()

    debug(f'MD5 for "{fname=}" is {md5val}')

    return md5val


def runcmd(cmd: str, args: list = None) -> bool:
    """Run a specific command on the SQLite DB"""

    result = False

    conn = connectdb()

    debug(f"conn = {conn}")
    debug(f"cmd  = {cmd}")
    debug(f"args = {args}")

    cursor = conn.cursor()
    try:
        if args is None:
            debug("invoking cursor.execute() without args")
            r = cursor.execute(cmd)
            debug(f"cursor.execute() returned {r}")
        else:
            debug("invoking cursor.execute() with args")
            r = cursor.execute(cmd, args)
            debug(f"cursor.execute() returned {r}")

        r = conn.commit()
        debug(f"conn.commit() returned {r}")

        rows = cursor.fetchall()
        numrows = len(list(rows))
        debug(f"{numrows=}")
        if numrows > 0:
            for row in rows:
                debug(f"  {row=}")
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


def inserthashtable(fname, md5, table: str = FILE_TABLE_NAME):
    """Insert into the SQLite File Table"""

    cmd = f"INSERT INTO {table} (fname, md5, moddate) VALUES (?, ?, ?)"
    args = (fname, md5, int(getmoddate(fname)))

    result = runcmd(cmd, args)
    debug(f"returning {result}")

    # r = runcmd("commit")
    # debug(f"commit returned {r}")

    return result

def updatehashtable(fname, md5, table: str = FILE_TABLE_NAME):
    """Update the SQLite File Table"""

    cmd = f"UPDATE {table} SET md5='{md5}', moddate={int(getmoddate(fname))} WHERE fname = '{fname}'"
    result = runcmd(cmd) #, args)
    debug(f"returning {result}")
    return result


def setuphashtable(fname, md5, table: str = FILE_TABLE_NAME):
    """Setup's the Hash Table"""

    if createhashtable(table):
        if createhashtableidx(table):
            if inserthashtable(fname, md5, table):
                debug(f"hash {table=} setup complete")
                debug(f"    inserted: {fname=}, {md5=}")
                return True
    error("Failed to setup hash {table=}")
    return False
    

def md5indb(fname, table: str = FILE_TABLE_NAME):
    """Checks if md5 hash tag exists in the SQLite DB"""

    result = False
    conn = connectdb()
    if conn is not None:
        try:
            query = f"SELECT md5 FROM {table} WHERE fname = ?"
            args = (fname, )
            result = corecursor(conn, query, args)
            debug(f"{result=}")
            result = True
        except sqlite3.OperationalError as err:
            error(str(err))
        finally:
            conn.close()

    return result

# To Be Implemented

def runfilechanges(ws):
    # Invoke the function that loads and parses the config file
    for i, fld in enumerate(fldexts[0]):
        # Invoke the function that checks each folder for file changes
        pass
    return changed

def checkfilechanges(folder, exclude, ws):
    changed = False
    """Checks for files changes"""
    for subdir, dirs, files in os.walk(folder):
        for fname in files:
            origin = os.path.join(subdir, fname)
            if os.path.isfile(origin): 
                      # Get file extension and check if it is not excluded
                      # Get the file’s md5 hash
                      # If the file has changed, add it to the Excel report
    return changed

def something():
    flds = []
    ext = []
    config = getbasefile() + '.ini'
    if os.path.isfile(config):
        cfile = open(config, 'r')
        # Parse each config file line and get the folder and extensions
    return flds, ext
