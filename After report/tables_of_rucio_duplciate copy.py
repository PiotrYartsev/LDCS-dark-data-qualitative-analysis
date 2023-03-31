from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from tqdm import *

from sqlalchemy import column
name='Lund_GRIDFTP_all_fixed_delete_all_copy.db'


con = sl.connect(name)

files_in_storage=open('C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\generate_data\\All_LUND_GRIDFTP\\files_found_storage.txt')
files_in_storage=files_in_storage.read()
files_in_storage=files_in_storage.split('\n')

for row in (con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #print(row[0])
        #get the first instance of a duplicate>0 and ComputingElement is not NULL
        all_duplicates=con.execute('SELECT id, file FROM '+row[0]+' WHERE ComputingElement IS NOT NULL').fetchall()
        if all_duplicates==[]:
            pass
        else:
            first_duplicate=all_duplicates[0][1]
            location=""
            if first_duplicate is not None:
                for line in files_in_storage:
                    if line == '':
                        pass
                    else:
                        if first_duplicate in line:
                            location=line.split(',')[3]
                            break
            #print(location)
            with con:
                con.execute('UPDATE '+row[0]+' SET DataLocation=?', (location,))
            get_new_location=con.execute('SELECT DataLocation FROM '+row[0]+' WHERE id=1').fetchone()
