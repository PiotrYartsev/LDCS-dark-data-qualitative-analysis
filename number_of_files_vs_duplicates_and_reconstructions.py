from multiprocessing.reduction import duplicate
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
duplicate_1={}
duplicate_2={}
not_a_duplicate={}
con = sl.connect('duplicate_data.db')
location_use=[]


max_number= 0
filenumberuplicate=[]
lengthoftable=[]
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #print(row[0])
        max_dup_number = con.execute("""
        SELECT MAX(file_number) from {};""".format(row[0])).fetchone()[0]
        if max_dup_number > max_number:
            max_number = max_dup_number
print(max_number)
"""
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #print(row[0])
        file_number = con.execute(""""""
        SELECT file_number FROM {} WHERE duplicate IS NOT NULL;"""""".format(row[0])).fetchall()
        if file_number==None:
            pass
        else:
            file_number=[o[0] for o in file_number]
            filenumberuplicate.extend(list(set(file_number)))
            #print(set(file_number))
"""
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #print(row[0])
        max_file_number= con.execute("""select MAX(file_number) from {};""".format(row[0])).fetchone()[0]
        file_number = con.execute("""
        SELECT file_number FROM {} WHERE duplicate IS NOT NULL;""".format(row[0])).fetchall()
        if file_number==None:
            pass
        else:
            lengthoftable.append(max_file_number)
            file_number=[o[0] for o in file_number]
            filenumberuplicate.append(len(file_number)/max_file_number)
            #print(set(file_number))
#print(filenumberuplicate)
splits=np.linspace(1,max_number,1000)
#print(splits)
plot_list=[]


filenumberuplicate= [x for _,x in sorted(zip(lengthoftable,filenumberuplicate))]
lengthoftable=(sorted(lengthoftable))


print(filenumberuplicate)
print(lengthoftable)
"""
for i in range(len(splits)):
    number=float(splits[i])
    j=[x for x in filenumberuplicate if not x > number]
    h=len(j)
    plot_list.append(h)
"""
plt.plot(lengthoftable,filenumberuplicate,".",label="Number of duplicates\nat filenumber\nless then limit", markersize=6, linewidth=0.5)
plt.grid(linestyle='--',)
plt.title("Procentage of duplicates for a sertain size of dataset",fontsize=20)
plt.xlabel('Procentage of files in dataset',fontsize=15)
plt.ylabel('Number of duplicates',   fontsize=15)
plt.legend(loc='upper left',fontsize=15)
plt.ylim(0)
plt.xlim(0)
#plt.yscale('log')
manager = plt.get_current_fig_manager()
manager.window.showMaximized()
plt.show()