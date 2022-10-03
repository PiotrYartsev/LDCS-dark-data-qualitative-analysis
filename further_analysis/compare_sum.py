import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 

from subprocess import PIPE, Popen

name='Lund_GRID_all.db'
con = sl.connect('/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/{}'.format(name))
#con=sl.connect("C:\\\\Users\\\\MSI PC\\\\Desktop\\\\gitproj\\\\LDCS-dark-data-qualitative-analysis\\\\{}".format(name))



def runner(input):
    for input2 in files_and_scopes[input]:
        scope=input2[1]
        file=input2[2]
        p = Popen("rucio get-metadata {}:{}".format(scope,file), shell=True, stdout=PIPE, stderr=PIPE)
        L_1, stderr = p.communicate()
        #print(type(L_1))
        stderr=stderr.decode("utf-8").split("\n")

        L=L_1.decode("utf-8").split("\n")  
        if len(stderr)>1:    
            print(stderr)
        
        if len(L)<2:
            compare_dict={}
        else:
            compare_dict={}
            for line in L:

                line=line.replace(" ","")
                line=line.split(":",1)
                if len(line)>1:
                    #print(line)
                    compare_dict[line[0]]=line[1]
        for key in compare_dict:
            if key in compare_dict2:
                if compare_dict[key] not in compare_dict2[key]:
                    compare_dict2[key].append(compare_dict[key])
            else:
                compare_dict2[key]=[compare_dict[key]]
            

statistics_dict={}
rows=[]
for row in (con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        has_duplicate=con.execute("SELECT * FROM {} where duplicate is not Null".format(row[0])).fetchall()
        if len(has_duplicate)>0:
            rows.append(row[0])
for row2 in (rows):
    print(row2)
    files_and_scopes={}
    Duplicates=con.execute("SELECT ComputingElement,scope,file,file_number FROM {} where  ComputingElement is not Null".format(row2)).fetchall()
    computing=[a[0] for a in Duplicates]
    scope=[a[1] for a in Duplicates]
    file_name=[a[2] for a in Duplicates]
    file_number_list=[a[3] for a in Duplicates]
    for n in range(len(computing)):
        file_number=file_number_list[n]
        if file_number not in files_and_scopes:
            files_and_scopes[file_number]=[]
            files_and_scopes[file_number].append((computing[n],scope[n],file_name[n]))
        else:
            files_and_scopes[file_number].append((computing[n],scope[n],file_name[n]))
    file_numbers=[]
    for file_number in files_and_scopes:
        file_numbers.append(file_number)
    compare_dict2={}
    files_to_sheck=file_numbers
    files_1=files_to_sheck[:len(files_to_sheck)//8]
    files_2=files_to_sheck[len(files_to_sheck)//8:len(files_to_sheck)*2//8]
    files_3=files_to_sheck[len(files_to_sheck)*2//8:len(files_to_sheck)*3//8]
    files_4=files_to_sheck[len(files_to_sheck)*3//8:len(files_to_sheck)*4//8]
    files_5=files_to_sheck[len(files_to_sheck)*4//8:len(files_to_sheck)*5//8]
    files_6=files_to_sheck[len(files_to_sheck)*5//8:len(files_to_sheck)*6//8]
    files_7=files_to_sheck[len(files_to_sheck)*6//8:len(files_to_sheck)*7//8]
    files_8=files_to_sheck[len(files_to_sheck)*7//8:]
    with tqdm(total=max(len(files_1),len(files_2),len(files_3),len(files_4),len(files_5),len(files_6),len(files_7),len(files_8))) as pbar:
        for (a, b, c,d,e,f,g,h) in itertools.zip_longest(files_1,files_2,files_3,files_4,files_5,files_6,files_7,files_8):
            t1 = threading.Thread(target=runner, args=(a,))
            t2 = threading.Thread(target=runner, args=(b,))
            t3 = threading.Thread(target=runner, args=(c,))
            t4 = threading.Thread(target=runner, args=(d,))
            t5 = threading.Thread(target=runner, args=(e,))
            t6 = threading.Thread(target=runner, args=(f,))
            t7 = threading.Thread(target=runner, args=(g,))
            t8 = threading.Thread(target=runner, args=(h,))

            if not a==None:
                t1.start()
            if not b==None:
                t2.start()
            if not c==None:
                t3.start()
            if not d==None:
                t4.start()
            if not e==None:
                t5.start()
            if not f==None:
                t6.start()
            if not g==None:
                t7.start()
            if not h==None:
                t8.start()
                
            if not a==None:
                t1.join()
            if not b==None:
                t2.join()
            if not c==None:
                t3.join()
            if not d==None:
                t4.join()
            if not e==None:
                t5.join()
            if not f==None:
                t6.join()
            if not g==None:
                t7.join()
            if not h==None:
                t8.join()

            pbar.update(1)
            

            
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
name2=name2.replace(" ","_")
plt.savefig('{}_different_metadata.png'.format(name2))