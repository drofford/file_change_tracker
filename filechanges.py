#!/usr/bin/env python

import os
import sys
import sqlite3

#
# globals
#

"""Returns the name of the SQLite DB file"""
def getbasefile():
    return os.path.splitext(os.path.basename(__file__))[0]

"""Connects to the SQLite DB"""
def connectdb() -> sqlite3.Connection:
    try:
        dbfile = getbasefile() + '.db'
        print("connectdb: DB file name is \"{}\"".format(dbfile))
        conn = sqlite3.connect(dbfile, timeout=2)
        print("connectdb: connected w/ conn = {}".format(conn))
        return conn
    except Exception as ex:
        sys.stderr.write("connectdb: Error: exception {} caught\n".format(ex))
        sys.stderr.flush()
        exit(1)

def tableexists(table: str) -> bool:
    """Checks if a SQLite DB Table exists"""
    result = False
    try:
        conn = connectdb()
        if conn is not None:
            print("tableexists: Connected to database with connection = {}\n".format(conn))

            query = "SELECT count(id) from {}".format(table) # Finish this query ...
            args = (table,)
            
            result = corecursor(conn, query)#, args)
            print("result = {}".format(result))

            return False

    except Exception as ex:
        sys.stderr.write("tableexists: exception of type \"{}\" caught".format(type(ex)))
        sys.stderr.write("tableexists: exception \"{}\" caught".format(ex))
        sys.stderr.flush()
        return False

def corecursor(conn: sqlite3.Connection , query: str, args: list=None) -> bool:
    try:
        cursor = conn.cursor()
        result = cursor.execute(query, args)
        print("corecursor: result = {}".format(result))
        return True
    except sqlite3.OperationalError as ex:
        sys.stderr.write("corecursor[1]: exception of type \"{}\" caught\n".format(type(ex)))
        sys.stderr.write("corecursor[1]: Error: exception \"{}\" caught\n".format(ex))
        return False
    except Exception as ex:
        sys.stderr.write("corecursor[2]: exception of type \"{}\" caught\n".format(type(ex)))
        sys.stderr.write("corecursor[2]: Error: exception \"{}\" caught\n".format(ex))
        sys.stderr.flush()
        exit(1)

def createhashtable(table: str="files") -> bool:
    result = False
    query = "CREATE TABLE {} if not exist (id integer primary key, file_name text)".format(table)
    try:
        conn = connectdb()
        if conn is not None:
            if tableexists(table):
                print("createhashtable: table {} does exist".format(table))
            else:
                print("createhashtable: table {} does not exist (yet)".format(table))
                try:
                    cursor = conn.cursor()
                    result = cursor.execute(query)
                    print("createhashtable: result = {}".format(result))
                    # To be continued ...
                except Exception as ex:
                    sys.stderr.write("createhashtable[1]: Error: exception \"{}\" caught\n".format(ex))
                    sys.stderr.flush()
                    exit(1)
    except Exception as ex:
        sys.stderr.write("createhashtable[2]: Error: exception \"{}\" caught\n".format(ex))
        sys.stderr.flush()
        exit(1)
    return result

def main():
    FILE_TABLE_NAME = "files"
    table_exists = tableexists(FILE_TABLE_NAME)
    print("main: table \"{}\" does{} exist\n".format(FILE_TABLE_NAME, "" if table_exists else " not")) 
    if not table_exists:
        result = createhashtable(FILE_TABLE_NAME)

#    cursor = conn.cursor()
#
#    cursor.execute("create table if not exists master (id integer primary key, filename text, last_accessed datetime)")
#    conn.commit()
    
if __name__ == "__main__":
    main()
