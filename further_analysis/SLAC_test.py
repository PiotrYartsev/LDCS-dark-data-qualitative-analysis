from itertools import count
from logging import raiseExceptions
from multiprocessing.connection import wait
from operator import index
from textwrap import indent
from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from tqdm import *
import os
from zlib import adler32
import time
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen


name='Lund_all_fixed_delete_all.db'
con = sl.connect(name)
#con=sl.connect("C:\\\\Users\\\\MSI PC\\\\Desktop\\\\gitproj\\\\LDCS-dark-data-qualitative-analysis\\\\{}".format(name))

not_rucio=[]
rucio=[]
batches=[]



for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else: 
        #print(row[0])

        x_values=[]
        y_values=[]
        duplicates=con.execute("SELECT ComputingElement FROM {} where duplicate is not Null".format(row[0])).fetchall()
        #print(len(duplicates))
        duplicates = [x[0] for x in duplicates]
        duplicates2=list(set(duplicates))
    
        for n in duplicates2:
            y_values.append(duplicates.count(n))
            if n==None:
                n="None"
            x_values.append(n)
        x_values.append("Sum of all duplicates in Rucio")
        if "None" in x_values:
            n=x_values.index("None")
            y_values.append(sum(y_values)-y_values[n])
        import pandas as pd

        name=name.replace(".db","")
        name2=name.replace("_all","")
        name2=name.replace("_2","")
        name2=name2.replace("_"," ")
        if "None" in x_values:
            rucio.append(y_values[x_values.index("Sum of all duplicates in Rucio")])
            not_rucio.append(y_values[x_values.index("None")])
            batches.append(row[0])
        
"""
data={}
#data['index']=["100%","90-100%"]
for n in range(len(rucio)):
    if rucio[n]/(not_rucio[n])==1:
        if "100%" in data:
            data["100%"]=data["100%"]+1
        else:
            data["100%"]=1
    elif rucio[n]/(not_rucio[n])>0.9 and rucio[n]/(not_rucio[n])<1:
        if "90-100%" in data:
            data["90-100%"]=data["90-100%"]+1
        else:
            data["90-100%"]=1
    elif rucio[n]/(not_rucio[n])>1 and rucio[n]/(not_rucio[n])<1.1:
        if "100-110%" in data:
            data["100-110%"]=data["100-110%"]+1
        else:
            data["100-110%"]=1
    elif rucio[n]/(not_rucio[n])>1.1:
        if "More then 110%" in data:
            data["More then 110%"]=data["More then 110%"]+1
        else:
            data["More then 110%"]=1
    elif rucio[n]/(not_rucio[n])<0.9:
        if "Less then 90%" in data:
            data["Less then 90%"]=data["Less then 90%"]+1
        else:
            data["Less then 90%"]=1
total_number_of_duplicates=sum(rucio)
print(len(data))

for n in range(len(rucio)):   
    print(batches[n]+" & " +str(rucio[n])+"  & "+str(not_rucio[n])+" & "+str(rucio[n]/(not_rucio[n])*100)+"%")


courses = list(data.keys())
values = list(data.values())
  
fig = plt.figure(figsize = (10, 5))
 
# creating the bar plot
plt.bar(courses, values, color ='tab:blue',
        width = 0.4)
 
plt.xlabel("Porcentage of duplicates in dataset %")
plt.title("{}: How many datasets have equal number of duplicates in Rucio and not in Rucio".format(name2))
name2=name2.replace(" ","_")
plt.savefig("further_analysis/plots/{}_100_procent.png".format(name2))
#plt.show()"""

data={}
data['index']=["100%","90-100%","100-110%", "More then 110%", "Less then 90%"]
data["All data"]=[0,0,0,0,0]
for n in range(len(rucio)):
    if rucio[n]/(not_rucio[n])==1:
        data["All data"][0]=data["All data"][0]+1
    elif rucio[n]/(not_rucio[n])>0.9 and rucio[n]/(not_rucio[n])<1:
        data["All data"][1]=data["All data"][1]+1
    elif rucio[n]/(not_rucio[n])>1 and rucio[n]/(not_rucio[n])<1.1:
        data["All data"][2]=data["All data"][2]+1
    elif rucio[n]/(not_rucio[n])>1.1:
        data["All data"][3]=data["All data"][3]+1
    elif rucio[n]/(not_rucio[n])<0.9:
        data["All data"][4]=data["All data"][4]+1
total_number_of_duplicates=sum(rucio)
print(len(data))

"""
for n in range(len(rucio)):   
    print(batches[n]+" & " +str(rucio[n])+"  & "+str(not_rucio[n])+" & "+str(rucio[n]/(not_rucio[n])*100)+"%")"""

total_number_of_duplicates=sum(data["All data"])
print(len(data))
for n in range(len(data["All data"])):
    data["All data"][n]=round(data["All data"][n]*100/total_number_of_duplicates,1)

not_rucio=[]
rucio=[]
batches=[]


for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else: 
        #print(row[0])
        max_dupl_value=con.execute("SELECT max(duplicate) FROM {}".format(row[0])).fetchall()
        if max_dupl_value[0][0] is not None:
            if max_dupl_value[0][0]>2:
                pass
            else:
                x_values=[]
                y_values=[]
                duplicates=con.execute("SELECT ComputingElement FROM {} where duplicate is not Null".format(row[0])).fetchall()
                #print(len(duplicates))
                duplicates = [x[0] for x in duplicates]
                duplicates2=list(set(duplicates))
            
                for n in duplicates2:
                    y_values.append(duplicates.count(n))
                    if n==None:
                        n="None"
                    x_values.append(n)
                x_values.append("Sum of all duplicates in Rucio")
                if "None" in x_values:
                    n=x_values.index("None")
                    y_values.append(sum(y_values)-y_values[n])
                import pandas as pd

                name=name.replace(".db","")
                name2=name.replace("_all","")
                name2=name.replace("_2","")
                name2=name2.replace("_"," ")
                if "None" in x_values:
                    rucio.append(y_values[x_values.index("Sum of all duplicates in Rucio")])
                    not_rucio.append(y_values[x_values.index("None")])
                    batches.append(row[0])

data["Max 2 duplicates"]=[0,0,0,0,0]
for n in range(len(rucio)):
    if rucio[n]/(not_rucio[n])==1:
        data["Max 2 duplicates"][0]=data["Max 2 duplicates"][0]+1
    elif rucio[n]/(not_rucio[n])>0.9 and rucio[n]/(not_rucio[n])<1:
        data["Max 2 duplicates"][1]=data["Max 2 duplicates"][1]+1
    elif rucio[n]/(not_rucio[n])>1 and rucio[n]/(not_rucio[n])<1.1:
        data["Max 2 duplicates"][2]=data["Max 2 duplicates"][2]+1
    elif rucio[n]/(not_rucio[n])>1.1:
        data["Max 2 duplicates"][3]=data["Max 2 duplicates"][3]+1
    elif rucio[n]/(not_rucio[n])<0.9:
        data["Max 2 duplicates"][4]=data["Max 2 duplicates"][4]+1
total_number_of_duplicates=sum(data["Max 2 duplicates"])
print(len(data))
for n in range(len(data["Max 2 duplicates"])):
    data["Max 2 duplicates"][n]=round(100*data["Max 2 duplicates"][n]/total_number_of_duplicates,1)






not_rucio=[]
rucio=[]
batches=[]


for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else: 
        x_values=[]
        y_values=[]
        duplicates=con.execute("SELECT ComputingElement FROM {} where duplicate is 1 OR duplicate is 2".format(row[0])).fetchall()
        #print(len(duplicates))
        duplicates = [x[0] for x in duplicates]
        duplicates2=list(set(duplicates))
    
        for n in duplicates2:
            y_values.append(duplicates.count(n))
            if n==None:
                n="None"
            x_values.append(n)
        x_values.append("Sum of all duplicates in Rucio")
        if "None" in x_values:
            n=x_values.index("None")
            y_values.append(sum(y_values)-y_values[n])
        import pandas as pd

        name=name.replace(".db","")
        name2=name.replace("_all","")
        name2=name.replace("_2","")
        name2=name2.replace("_"," ")
        if "None" in x_values:
            rucio.append(y_values[x_values.index("Sum of all duplicates in Rucio")])
            not_rucio.append(y_values[x_values.index("None")])
            batches.append(row[0])

data["Only first 2 duplicates"]=[0,0,0,0,0]
for n in range(len(rucio)):
    if rucio[n]/(not_rucio[n])==1:
        data["Only first 2 duplicates"][0]=data["Only first 2 duplicates"][0]+1
    elif rucio[n]/(not_rucio[n])>0.9 and rucio[n]/(not_rucio[n])<1:
        data["Only first 2 duplicates"][1]=data["Only first 2 duplicates"][1]+1
    elif rucio[n]/(not_rucio[n])>1 and rucio[n]/(not_rucio[n])<1.1:
        data["Only first 2 duplicates"][2]=data["Only first 2 duplicates"][2]+1
    elif rucio[n]/(not_rucio[n])>1.1:
        data["Only first 2 duplicates"][3]=data["Only first 2 duplicates"][3]+1
    elif rucio[n]/(not_rucio[n])<0.9:
        data["Only first 2 duplicates"][4]=data["Only first 2 duplicates"][4]+1
total_number_of_duplicates=sum(rucio)
print(len(data))







total_number_of_duplicates=sum(data["Only first 2 duplicates"])
print(len(data))
for n in range(len(data["Only first 2 duplicates"])):
    data["Only first 2 duplicates"][n]=round(100*data["Only first 2 duplicates"][n]/total_number_of_duplicates,1)









import pandas as pd
print(data)
df = pd.DataFrame(data)
print(df)
df.set_index('index', drop=True, inplace=True)

name=name.replace(".db","")
name2=name.replace("_all","")
name2=name.replace("_2","")
name2=name2.replace("_"," ")
df.plot(kind="bar",figsize=(10, 10))
plt.xticks(rotation='horizontal')

#plt.show()
manager = plt.get_current_fig_manager()
manager.window.showMaximized()
#plt.show()
figure = plt.gcf()
figure.set_size_inches(19, 10)
plt.ylim(0,100)
plt.ylabel("Percentage of duplicates")
plt.xlabel("Porcentage of duplicates in dataset %")
plt.title("{}: How many datasets have equal number of duplicates in Rucio and not in Rucio".format(name2))
name2=name2.replace(" ","_")
plt.savefig("further_analysis/plots/{}_100_procent.png".format(name2))
