import statistics
from tqdm import *
import sqlite3 as sl

from subprocess import PIPE, Popen

name='SLAC_mc20_2.db'
con = sl.connect('/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/{}'.format(name))
#con=sl.connect("C:\\\\Users\\\\MSI PC\\\\Desktop\\\\gitproj\\\\LDCS-dark-data-qualitative-analysis\\\\{}".format(name))



def runner(scope,file):
    p = Popen("rucio get-metadata {}:{}".format(scope,file), shell=True, stdout=PIPE, stderr=PIPE)
    L_1, stderr = p.communicate()
    #print(type(L_1))
    stderr=stderr.decode("utf-8").split("\n")

    L=L_1.decode("utf-8").split("\n")  
    if len(stderr)>1:    
        print(stderr)
    
    if len(L)<2:
        pass
    else:
        compare_dict={}
        for line in L:

            line=line.replace(" ","")
            line=line.split(":",1)
            if len(line)>1:
                #print(line)
                compare_dict[line[0]]=line[1]
    return compare_dict
            




statistics_dict={}
rows=[]
for row in (con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        has_duplicate=con.execute("SELECT * FROM {} where duplicate is not Null".format(row[0])).fetchall()
        if len(has_duplicate)>0:
            rows.append(row[0])
for row2 in tqdm(rows):
    
    print(row2)
    files_and_scopes={}
    list_of_file_number=con.execute("SELECT file_number FROM {} where duplicate is not Null".format(row2)).fetchall()
    list_of_file_number = [x[0] for x in list_of_file_number]
    list_of_file_number=list(set(list_of_file_number))
    if len(list_of_file_number)>0:
        for file_number in list_of_file_number:
            #print(file_number)
            Duplicates=con.execute("SELECT ComputingElement,scope,file FROM {} where file_number={} and ComputingElement is not Null".format(row2,file_number)).fetchall()
            computing=[a[0] for a in Duplicates]
            scope=[a[1] for a in Duplicates]
            file_name=[a[2] for a in Duplicates]
            for n in range(len(computing)):
                #print(computing[n],scope[n],file_name[n])
                if file_number not in files_and_scopes:
                    files_and_scopes[file_number]=[]
                    files_and_scopes[file_number].append((computing[n],scope[n],file_name[n]))
                else:
                    files_and_scopes[file_number].append((computing[n],scope[n],file_name[n]))

    for file_number in files_and_scopes:
        compare_dict2={}
        
        for n in range(len(files_and_scopes[file_number])):
            scope=files_and_scopes[file_number][n][1]
            file=files_and_scopes[file_number][n][2]
            compare_dict=runner(scope,file)
            for key in compare_dict:
                if key in compare_dict2:
                    if compare_dict[key] not in compare_dict2[key]:
                        compare_dict2[key].append(compare_dict[key])
                else:
                    compare_dict2[key]=[compare_dict[key]]
        for key in compare_dict2:
            if len(compare_dict2[key])>1:
                #print("             ",key,compare_dict2[key])     
                if key in statistics_dict:
                    statistics_dict[key]=statistics_dict[key]+1
                else:
                    statistics_dict[key]=1
print(statistics_dict)
#plot a bar chart
import tkinter
import matplotlib.pyplot as plt
import numpy as np
import operator
name=name.replace(".db","")
name2=name.replace("_all","")
name2=name.replace("_2","")
name2=name2.replace("_"," ")
plt.bar(range(len(statistics_dict)), list(statistics_dict.values()), align='center')
plt.xticks(range(len(statistics_dict)), list(statistics_dict.keys()),rotation=90)
plt.ylabel('Number of files')
plt.title('{}: Number of files with different metadata'.format(name2))
plt.show()



            
            
            
            
            

            
            
            
            

            
            

            
            

            
            
            
            
            
            
            
            
            
             



