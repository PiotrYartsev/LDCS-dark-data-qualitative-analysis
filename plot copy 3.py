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
duplicate_1={}
duplicate_2={}
not_a_duplicate={}
con = sl.connect('duplicate_data.db')
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])

        data = con.execute("SELECT ComputingElement FROM {} WHERE duplicate = 1".format(row[0]))
        for rows1 in data:
            if rows1[0] in duplicate_1:
                duplicate_1[rows1[0]]+=1
            else:
                duplicate_1[rows1[0]]=1
        data2 = con.execute("SELECT ComputingElement FROM {} WHERE duplicate = 2".format(row[0]))
        for rows2 in data2:
            if rows2[0] in duplicate_2:
                duplicate_2[rows2[0]]+=1
            else:
                duplicate_2[rows2[0]]=1
        data3 = con.execute("SELECT ComputingElement FROM {}".format(row[0]))
        for rows3 in data3:
            if rows3[0] in not_a_duplicate:
               not_a_duplicate[rows3[0]]+=1
            else:
                not_a_duplicate[rows3[0]]=1


        
        
# creating the dataset

courses = []
values = []


for key in duplicate_1:
    courses.append(key)
   

    values.append(duplicate_2[key]/ not_a_duplicate[key])
fig = plt.figure(figsize = (10, 5))

# creating the bar plot
plt.bar(courses, values, color ='maroon',
        width = 0.4)
        
plt.xlabel("Computational centers")
plt.ylabel("Procentage of duplicates")
plt.title("procentage of duplicates belonging\n to differet computational centers")
plt.show()