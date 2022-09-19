from audioop import add
import os
import time
from datetime import datetime

import sqlite3 as sl

con = sl.connect('duplicate_data_copy_3.db')

for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else: 
        print(row[0])
        with con:
            con.execute("""if """.format(row[0]))
