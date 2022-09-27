
from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl

from sqlalchemy import column
from sympy import linsolve


name='Lund_GRID_all.db'
con = sl.connect('{}'.format(name))
max_file_number_list=[]
procentage_of_duplicates=[]

for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        max_file_number=con.execute('Select id from {};'.format(row[0])).fetchall()
        max_file_number=[i[0] for i in max_file_number]
        max_file_number=len(max_file_number)
        number_of_duplicates=con.execute('Select duplicate from {} where duplicate is not Null;'.format(row[0])).fetchall()
        number_of_duplicates=[i[0] for i in number_of_duplicates]
        number_of_duplicates=len(number_of_duplicates)
        if max_file_number>10000:
            pass
        else:
            procentage_of_duplicates.append(number_of_duplicates/max_file_number)
            max_file_number_list.append(max_file_number)

print(len(max_file_number_list))
print(len(procentage_of_duplicates))
#arrange the data
max_file_number_list,number_of_duplicates_list=zip(*sorted(zip(max_file_number_list,procentage_of_duplicates)))

plt.plot(max_file_number_list,procentage_of_duplicates)
plt.xlabel('Number of files')
plt.ylabel('Number of duplicates')
name=name.replace('.db','')
name=name.replace('_all','')
name=name.replace('_',' ')
plt.title('Number of files vs number of duplicates in {}'.format(name))
plt.show()

