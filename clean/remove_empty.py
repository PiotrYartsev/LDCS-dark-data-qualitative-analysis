from tokenize import Number


import sqlite3 as sl

def remove_empty(database):
    con = sl.connect(database)
    location_use=[]
    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else:
            number_of_files=con.execute('SELECT COUNT(*) FROM '+row[0]).fetchone()[0]
            if number_of_files==0:
                #delete table
                print(row[0])
                con.execute('DROP TABLE '+row[0])
            else:
                pass