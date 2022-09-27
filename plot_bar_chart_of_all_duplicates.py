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
name='SLAC_mc20_2.db'
con = sl.connect('{}'.format(name))
location_use=[]
number_of_duplicates=[]
number_of_files=[]
number_of_first_duplicates=[]
number_of_missing_from_rucioc=[]
largest_dup_chain=[0,0]
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        max_dup_number = con.execute("""
        SELECT MAX(duplicate) FROM {};""".format(row[0])).fetchone()[0]

        number_of_duplicates_indatabase=con.execute('SELECT COUNT(*) FROM {} where duplicate is not Null;'.format(row[0])).fetchone()[0]
        
        number_of_first_duplicates_indatabase=con.execute('SELECT COUNT(*) FROM {} where duplicate is 1;'.format(row[0])).fetchone()[0]
        
        number_of_missing_from_rucio=con.execute('SELECT COUNT(*) FROM {} where ComputingElement is Null;'.format(row[0])).fetchone()[0]

        max_number = con.execute("""
        SELECT MAX(id) FROM {};""".format(row[0])).fetchone()[0]
        
        max__duplicate_number = con.execute("""
        SELECT MAX(duplicate) FROM {};""".format(row[0])).fetchone()[0]
        print(number_of_first_duplicates_indatabase)
        if max_dup_number==None:
            pass
        else:
            #print(max_dup_number)
            number_of_duplicates.append(number_of_duplicates_indatabase)
            #print(max_number)
            number_of_files.append(max_number)
            number_of_first_duplicates.append(number_of_first_duplicates_indatabase)
            number_of_missing_from_rucioc.append(number_of_missing_from_rucio)
            if max_dup_number>largest_dup_chain[0]:
                largest_dup_chain=[max_dup_number,row[0]]
            
            
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

name=name.replace(".db","")
name2=name.replace("_all","")
name2=name.replace("_2","")
name2=name2.replace("_"," ")
df.plot(kind="bar",figsize=(10, 10))
plt.xticks(rotation='horizontal')
plt.title("{}: Procentage that is duplicate at different computing element".format(name2))

plt.xlabel("Computing center")

plt.ylabel("Procentage of files")
#plt.show()
manager = plt.get_current_fig_manager()
manager.window.showMaximized()
#plt.show()
figure = plt.gcf()
figure.set_size_inches(19, 10)

if not os.path.exists("figures/{}/bar-plot".format(name)):
    os.makedirs("figures/{}/bar-plot".format(name))
plt.savefig("figures/{}/bar-plot/{}_procentage.png".format(name,name2),bbox_inches='tight', dpi=100)
plt.close()
df.to_csv('figures/{}/bar-plot/procentage.csv'.format(name), index=True)


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
            #duplicate_2[string_to_write].append(number_j/not_a_duplicate[j])
            duplicate_2[string_to_write].append(number_j)
        else:
            duplicate_2[string_to_write]=[]
            duplicate_2[string_to_write].append(number_j)
           
            #duplicate_2[string_to_write].append(number_j/not_a_duplicate[j])
            


   
import pandas as pd
        
df = pd.DataFrame(duplicate_2)
print(df)
df.set_index('index', drop=True, inplace=True)


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
figure.set_size_inches(19, 10)
name=name.replace(".db","")
if not os.path.exists("figures/{}/bar-plot".format(name)):
    os.makedirs("figures/{}/bar-plot".format(name))
plt.savefig("figures/{}/bar-plot/{}_number_of.png".format(name,name2),bbox_inches='tight', dpi=100)
plt.close()
df.to_csv('figures/{}/bar-plot/number_of.csv'.format(name), index=True)

print(name2)
print("Number of files")
print(sum(number_of_files))
print("Number of duplicates")
print(sum(number_of_duplicates))
print("Number of files missing from Rucio")
print(sum(number_of_missing_from_rucioc))
print("procentage of duplicates")
print((sum(number_of_duplicates)/sum(number_of_files))*100)
print("largest chain of duplicates")
print(largest_dup_chain)