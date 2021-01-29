#!/usr/bin/env python

import re
import sqlite3

DB_FILE_NAME = "sparky.db"
TABLE_NAME = "status"

def connect(dbfile: str) -> sqlite3.Connection:
    print(f'connecting to D/B file with name "{dbfile}"')
    try:
        conn = sqlite3.connect(dbfile, timeout=2)
        print(f"success: {conn=}")
        return conn

    except Exception as ex:
        print(f"fork it: the shirt has really hit the fan: {ex=}")
        exit(0)


def create_table(conn, table_name) -> None:
    """Creates the named table if it does not exist"""
    result = None

    cmd = f"CREATE TABLE {table_name} (id integer primary key, fname text, md5 text, moddate integer)"
    print(f"command = {cmd}")

    cursor = conn.cursor()
    try:
        print("invoking cursor.execute() without args")
        r = cursor.execute(cmd)
        print(f"cursor.execute() returned {r}")
        r = conn.commit()
        print(f"conn.commit() returned {r}")

        rows = cursor.fetchall()
        numrows = len(list(rows))
        print(f"{numrows=}")
        if numrows > 0:
            for row in rows:
                print(f"  {row=}")
        result = True

    except sqlite3.IntegrityError as err:
        print(str(err))

    except sqlite3.OperationalError as err:
        if len(re.findall(r"table\s+(\S+)\s+already\s+exists", str(err))) > 0:
            result = False
        print(str(err))

    finally:
        cursor.close()

    print(f'{"create new" if result else "using extant"} table "{table_name}"')
    return result


def insert_record(conn, table_name, fname, md5, mod_date):
    result = None

    cmd = f"INSERT INTO {table_name} (fname, md5, moddate) VALUES ('{fname}', '{md5}', {mod_date})"
    print(f"command = {cmd}")



def main():
    conn = connect(DB_FILE_NAME)
    print(f"{conn=}")

    create_table(conn, TABLE_NAME)

    insert_record(conn, TABLE_NAME, "FILE1.TXT", "md5-1", 1)

    conn.close()


if __name__ == "__main__":
    main()
