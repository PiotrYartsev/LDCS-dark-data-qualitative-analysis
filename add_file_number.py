from audioop import add
import os

from datetime import datetime

import sqlite3 as sl

con = sl.connect('SLAC_mc20_2.db')

for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])

        add_file_number=[]

        if row[0]=="EaTtest":
            with con:
                con.execute("""
                DROP TABLE {};""".format(row[0]))
        else:
            with con:
                con.execute("""
                DELETE FROM {}
                    WHERE file IS NULL;""".format(row[0]))

            with con:
                con.execute("""
                ALTER TABLE {}
                    ADD file_number INTEGER;""".format(row[0]))
                

            data = con.execute("SELECT file FROM {}".format(row[0]))
            
            for rows in data:
                #print(rows[0])
                address_to_change=rows[0].split("_")[-2].replace("run","")
                #print(address_to_change)
                add_file_number.append(address_to_change)

            add_file_number_2=[]
            for a in add_file_number:
                add_file_number_2.append((a))
            




            with con:
                
                for n in range(len(add_file_number_2)):
                    
                    sql="update {} set file_number=({}) where id={};".format(row[0],add_file_number_2[n],n+1)
                    #print(sql)
                    con.execute(sql)
