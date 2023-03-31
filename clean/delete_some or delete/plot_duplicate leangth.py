from cProfile import label
from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
import tqdm as tqdm

from sqlalchemy import column

name='Lund_GRIDFTP_all_fixed_delete_all.db'
con = sl.connect('{}'.format(name))

largest_dup_chain=[0,0]
max_values=[]
acourances=[]
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        file_numbers=con.execute('Select file_number from {};'.format(row[0])).fetchall()
        file_numbers=[i[0] for i in file_numbers]
        file_numbers=list(set(file_numbers))
        for filenumber in file_numbers: 
            data = con.execute("SELECT duplicate FROM {} WHERE file_number is {}".format(row[0],filenumber)).fetchall()
            data=[i[0] for i in data]
            max_value_for_duplicate=max(data)
            if max_value_for_duplicate in max_values:
                acourances[max_values.index(max_value_for_duplicate)]+=1
            else:
                max_values.append(max_value_for_duplicate)
                acourances.append(1)
            
#ORDER THE DATA
max_values,acourances=zip(*sorted(zip(max_values,acourances)))
plt.plot(max_values,acourances)
plt.xlabel('Number of duplicates in duplicate group')
plt.ylabel('Number of of occurences')
name=name.replace('.db','')
name=name.replace('_all','')
name=name.replace('_',' ')
plt.title('Lenagth of duplicate groups in {}'.format(name))
plt.show()