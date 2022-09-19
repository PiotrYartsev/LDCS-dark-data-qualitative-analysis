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
con = sl.connect('Lund_all.db')
location_use=[]
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        max_dup_number = con.execute("""
        SELECT MAX(duplicate) FROM {};""".format(row[0])).fetchone()[0]
        if max_dup_number==None:
            pass
        else:
            #print(max_dup_number)

            for i in range(1,max_dup_number+1):
                #print(i)
                data = con.execute("SELECT ComputingElement FROM {} WHERE duplicate is {}".format(row[0],i)).fetchall()
                #print(len(data))
                for rows1 in data:
                    if rows1[0]==None:
                        pass
                    if not rows1[0]==None:
                        location=str(rows1[0]).replace(" ","")
                    else:
                        location="None"
                    if i in duplicate_1:
                        duplicate_1[i].append(location)
                    else:
                        duplicate_1[i]=[]
                        duplicate_1[i].append(location)
                        
            data3 = con.execute("SELECT ComputingElement FROM {}".format(row[0]))
            for rows3 in data3:
                if str(rows3[0]) in not_a_duplicate:
                    not_a_duplicate[str(rows3[0])]+=1
                else:
                    not_a_duplicate[str(rows3[0])]=1
            locations = con.execute("SELECT DISTINCT ComputingElement FROM {}".format(row[0],i)).fetchall()

            for stuff in locations:
                location_use.append(str(stuff[0]).replace(" ",""))

duplicate_2={}
j_list=[]
location_use=list(set(location_use))
print(location_use)
for i in duplicate_1:
    #print(i)
    data=(duplicate_1[i])
    duplicate_2['index']=[]
    #print(data2)
    for j in location_use:
        
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
            #duplicate_2[string_to_write].append(number_j)
        else:
            duplicate_2[string_to_write]=[]
            #duplicate_2[string_to_write].append(number_j)
           
            duplicate_2[string_to_write].append(number_j/not_a_duplicate[j])
            


   
import pandas as pd
        
df = pd.DataFrame(duplicate_2)
print(df)
df.set_index('index', drop=True, inplace=True)


df.plot(kind="bar",figsize=(10, 10))
plt.xticks(rotation='horizontal')
plt.title("Procentage that is duplicate at different computing element")

plt.xlabel("Computing center")

plt.ylabel("Number of files")
plt.show()
"""
print(not_a_duplicate)
df2 = pd.DataFrame(not_a_duplicate,index=[0])
print(df2)

df2.plot(kind="bar",figsize=(10, 10))
plt.xticks(rotation='horizontal')
plt.title("Duplicates procentage of all and computing element")

plt.xlabel("Computing center")

plt.ylabel("Number of files")
plt.show()"""