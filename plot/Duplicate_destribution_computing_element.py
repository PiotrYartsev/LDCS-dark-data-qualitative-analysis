import os
import sqlite3 as sl
from datetime import datetime
from tokenize import Number
import matplotlib.ticker as mtick

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axis import YAxis
from matplotlib.ticker import MultipleLocator
from sqlalchemy import column


name='SLAC_mc20_delete_all.db'

plt.rcParams.update({'font.size': 18})


con = sl.connect(name)
duplicate_locations={}
non_duplicate_locations={}
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        duplicate_list=con.execute('SELECT ComputingElement,duplicate FROM {} where duplicate is not Null;'.format(row[0])).fetchall()
        duplicate_list=[x if x[0] is not None else ["none",x[1]] for x in duplicate_list]
        duplicate_list=[[x[0].replace(" ",""),x[1]] for x in duplicate_list]
        
        for thing in duplicate_list:
            if thing[0] in duplicate_locations:
                if thing[1] in duplicate_locations[thing[0]]:
                    duplicate_locations[thing[0]][thing[1]]+=1
                else:
                    duplicate_locations[thing[0]][thing[1]]=1
            else:
                duplicate_locations[thing[0]]={}
                duplicate_locations[thing[0]][thing[1]]=1



        non_duplicate_list=con.execute('SELECT ComputingElement FROM {} where duplicate is Null;'.format(row[0])).fetchall()
        non_duplicate_list=[x[0]for x in non_duplicate_list]
        non_duplicate_list=[x if x is not None else "none" for x in non_duplicate_list]
        non_duplicate_list=[x.replace(" ","") for x in non_duplicate_list]
        for thing in non_duplicate_list:
            if thing in non_duplicate_locations:
                non_duplicate_locations[thing]+=1
            else:
                non_duplicate_locations[thing]=1
        
con.close()
import pandas as pd

print(non_duplicate_locations)


all_locations={}
for key in duplicate_locations:
    sum = 0
    for key2 in duplicate_locations[key]:
        sum+=duplicate_locations[key][key2]
    all_locations[key]=sum+non_duplicate_locations[key]
print(all_locations)



procentage_locations={}
for key in duplicate_locations:
    procentage_locations[key]={}
    for key2 in duplicate_locations[key]:
        procentage_locations[key][key2]=100*duplicate_locations[key][key2]/(all_locations[key])





for key in duplicate_locations:
    for key2 in list(duplicate_locations[key]):
        if key2==1:
            #make it say 1'st
            duplicate_locations[key]['1\'st']=duplicate_locations[key][key2]
            del duplicate_locations[key][key2]
        elif key2==2:
            duplicate_locations[key]['2\'nd']=duplicate_locations[key][key2]
            del duplicate_locations[key][key2]
        elif key2==3:
            duplicate_locations[key]['3\'rd']=duplicate_locations[key][key2]
            del duplicate_locations[key][key2]
        else:
            duplicate_locations[key][str(key2)+'\'th']=duplicate_locations[key][key2]
            del duplicate_locations[key][key2]


for key in procentage_locations:
    for key2 in list(procentage_locations[key]):
        if key2==1:
            #make it say 1'st
            procentage_locations[key]['1\'st']=procentage_locations[key][key2]
            del procentage_locations[key][key2]
        elif key2==2:
            procentage_locations[key]['2\'nd']=procentage_locations[key][key2]
            del procentage_locations[key][key2]
        elif key2==3:
            procentage_locations[key]['3\'rd']=procentage_locations[key][key2]
            del procentage_locations[key][key2]
        else:
            procentage_locations[key][str(key2)+'\'th']=procentage_locations[key][key2]
            del procentage_locations[key][key2]

df1=pd.DataFrame(procentage_locations)
df1=df1.fillna(0)
df1=df1.astype(float)
df1=df1.transpose()
name=name.replace(".db","")
name2=name.replace("_all","")
name2=name.replace("_2","")
name2=name2.replace("_"," ")
df1.plot(kind="bar",figsize=(10, 10))
plt.xticks(rotation='horizontal')
plt.title("{}: Procentage that is duplicate at different computing element".format(name2))


plt.xlabel("Computing center")

plt.ylabel("Procentage of files")
#plt.show()
manager = plt.get_current_fig_manager()
manager.window.showMaximized()
#plt.show()
figure = plt.gcf()
figure.set_size_inches(22, 10)
#make y-axis start show precets
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())

#for each location in df print the sum off all procentages
for i in range(0,len(df1.index)):
    print(df1.index[i],df1.iloc[i].sum())


if not os.path.exists("figures/{}/bar-plot".format(name)):
    os.makedirs("figures/{}/bar-plot".format(name))


plt.savefig("figures/{}/bar-plot/{}_procentage.png".format(name,name2),bbox_inches='tight', dpi=100)
plt.close()
df1.to_csv('figures/{}/bar-plot/procentage.csv'.format(name), index=True)

df = pd.DataFrame(duplicate_locations)
df = df.fillna(0)
df = df.astype(int)
df = df.transpose()

df.plot(kind="bar",figsize=(10, 10))
plt.xticks(rotation='horizontal')
plt.title("{}: Number of duplicates at different computing element".format(name2))

plt.xlabel("Computing center")

plt.ylabel("Number of files")
#plt.show()
manager = plt.get_current_fig_manager()
manager.window.showMaximized()
#plt.show()
figure = plt.gcf()
figure.set_size_inches(22, 10)
name=name.replace(".db","")
if not os.path.exists("figures/{}/bar-plot".format(name)):
    os.makedirs("figures/{}/bar-plot".format(name))
plt.savefig("figures/{}/bar-plot/{}_number_of.png".format(name,name2),bbox_inches='tight', dpi=100)
plt.close()
df.to_csv('figures/{}/bar-plot/number_of.csv'.format(name), index=True)