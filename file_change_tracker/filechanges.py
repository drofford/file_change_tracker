#!/usr/bin/env python

import os
import sys
import sqlite3

"""Returns the name of the SQLite DB file"""
def getbasefile():
    return os.path.splitext(os.path.basename(__file__))[0]

"""Connects to the SQLite DB"""
def connectdb():
    try:
        dbfile = getbasefile() + '.db'
        print("DB file name is \"{}\"".format(dbfile))
        conn = sqlite3.connect(dbfile, timeout=2)
        cursor = conn.cursor()

        cursor.execute("create table if not exists master (id integer primary key, filename text, last_accessed datetime)")
        conn.commit()
        
        return conn
    except Exception as ex:
        sys.stderr.write("Error: exception {} caught".format(ex))
        exit(1)
    
def main():
    conn = connectdb()
    print("Connected to database with connection = {}".format(conn))

if __name__ == "__main__":
    main()
