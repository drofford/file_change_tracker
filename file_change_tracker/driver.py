#!/usr/bin/env python

from filechanges import connectdb, tableexists
    
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
