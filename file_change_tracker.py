#!/usr/bin/env python

import os
import sys
import sqlite3

#
# globals
#
conn = None

"""Returns the name of the SQLite DB file"""
def getbasefile():
    return os.path.splitext(os.path.basename(__file__))[0]

"""Connects to the SQLite DB"""
def connectdb():
	global conn
    try:
        dbfile = getbasefile() + '.db'
        print("DB file name is \"{}\"".format(dbfile))
        conn = sqlite3.connect(dbfile, timeout=2)
    except Exception as ex:
        sys.stderr.write("Error: exception {} caught".format(ex))
        exit(1)

def tableexists(table):
    """Checks if a SQLite DB Table exists"""
    result = False
    try:
        conn = connectdb()
        if not conn is None:
                   query = "SELECT name … AND name=?" # Finish this query ...
                   args = (table,)
                   result = corecursor(conn, query, args)
	      # To be continued ...
    except Exception as ex:
        sys.stderr.write("Error: exception {} caught".format(ex))
        exit(1)


def main():
    conn = connectdb()
    print("Connected to database with connection = {}".format(conn))
    
    table_exists = tableexists(conn, "master")
    print("table \"{}\" does{} exist".format("master", "" if table_exists else " not")) 

#    cursor = conn.cursor()
#
#    cursor.execute("create table if not exists master (id integer primary key, filename text, last_accessed datetime)")
#    conn.commit()
    
if __name__ == "__main__":
    main()