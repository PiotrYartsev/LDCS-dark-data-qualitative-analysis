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
con = sl.connect('duplicate_data_2(1).db')
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #print(row[0])
        max_dup_number = con.execute("""
        SELECT MAX(duplicate) FROM {};""".format(row[0])).fetchone()[0]
        if max_dup_number==None:
            pass
        else:
            #print(max_dup_number)

            for i in range(1,max_dup_number+1):
                #print(i)
                data = con.execute("SELECT ComputingElement FROM {} WHERE duplicate = {}".format(row[0],i))

                for rows1 in data:
                    if i in duplicate_1:
                        duplicate_1[i].append(rows1[0])
                    else:
                        duplicate_1[i]=[]
                        duplicate_1[i].append(rows1[0])
            data3 = con.execute("SELECT ComputingElement FROM {}".format(row[0]))
            for rows3 in data3:
                if rows3[0] in not_a_duplicate:
                    not_a_duplicate[rows3[0]]+=1
                else:
                    not_a_duplicate[rows3[0]]=1

duplicate_2={}
j_list=[]
for i in duplicate_1:
    #print(i)
    data=(duplicate_1[i])
    data1=(duplicate_1[1])
    data2=list(set(data1))
    duplicate_2['index']=[]
    #print(duplicate_2)
    for j in data2:
        duplicate_2['index'].append(j)
        number_j=data.count(j)
        if i==1:
            string_to_write=str(i)+'st file'
        elif i==2:
            string_to_write=str(i)+'nd file'
        elif i==3:
            string_to_write=str(i)+'rd file'
        else:
            string_to_write=str(i)+'th file'
        if string_to_write in duplicate_2:
            duplicate_2[string_to_write].append(number_j/not_a_duplicate[j])
        else:
            duplicate_2[string_to_write]=[]
            duplicate_2[string_to_write].append(number_j/not_a_duplicate[j])
            


#print(duplicate_1)
#print(duplicate_2)
        
import pandas as pd
        
df = pd.DataFrame(duplicate_2)
print(df)
df.set_index('index', drop=True, inplace=True)


df.plot(kind="bar",figsize=(10, 10))
plt.xticks(rotation='horizontal')
plt.title("Duplicates procentage of all and computing element")

plt.xlabel("Computing center")

plt.ylabel("Number of files")
plt.show()

print(not_a_duplicate)
df2 = pd.DataFrame(not_a_duplicate,index=[0])
print(df2)

df2.plot(kind="bar",figsize=(10, 10))
plt.xticks(rotation='horizontal')
plt.title("Duplicates procentage of all and computing element")

plt.xlabel("Computing center")

plt.ylabel("Number of files")
plt.show()