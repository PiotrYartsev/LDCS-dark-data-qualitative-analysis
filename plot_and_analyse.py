import os

from datetime import datetime

import sqlite3 as sl

con = sl.connect('duplicate_data.db')


#with con:
 #   con.execute("""
  #  ALTER TABLE USER
   #     ADD file_number INTEGER;""")

data = con.execute("SELECT file FROM USER")
for row in data:
    print(row)
    

