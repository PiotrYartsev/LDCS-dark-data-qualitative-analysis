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
name='Lund_all_delete_all.db'
con = sl.connect(name)
count_number_one_duplicate_list=0
count_all_duplicates_list=0

for row in tqdm(con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #print(row[0])
        count_number_one_duplicate=con.execute('SELECT COUNT(*) FROM '+row[0]+' WHERE duplicate=1').fetchall()
        count_all_duplicates=con.execute('SELECT COUNT(*) FROM '+row[0]+' WHERE duplicate>0').fetchall()
        count_number_one_duplicate=count_number_one_duplicate[0][0]
        count_all_duplicates=count_all_duplicates[0][0]
        count_number_one_duplicate_list=count_number_one_duplicate_list+count_number_one_duplicate
        count_all_duplicates_list=count_all_duplicates_list+count_all_duplicates

print('This is for the dataset: '+name)
print('Total number of first duplicate: '+str(count_number_one_duplicate_list))
print('Total number of all duplicates: '+str(count_all_duplicates_list))

print('Precentage of one duplicate: '+str(round(count_number_one_duplicate_list/count_all_duplicates_list*100,1))+ '%')