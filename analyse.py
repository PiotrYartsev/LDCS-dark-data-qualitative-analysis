from audioop import add
import os

from datetime import datetime

import sqlite3 as sl

con = sl.connect('duplicate_data.db')




add_file_number=[]
with con:
     con.execute("""
     ALTER TABLE USER
        DROP COLUMN file_number;""")

with con:
    con.execute("""
    DELETE FROM USER
        WHERE file IS NULL;""")

with con:
    con.execute("""
    ALTER TABLE USER
        ADD file_number INTEGER;""")
     

data = con.execute("SELECT file FROM USER")
print(type(data))
for row in data:
    address_to_change=row[0].split("_")[3].replace("run","")
    add_file_number.append(address_to_change)

add_file_number_2=[]
for a in add_file_number:
    add_file_number_2.append((a))
print(add_file_number_2)




with con:
    for n in range(len(add_file_number_2)):
        
        sql="update USER set file_number=({}) where id={}".format(add_file_number_2[n],n+1)
        con.execute(sql)
