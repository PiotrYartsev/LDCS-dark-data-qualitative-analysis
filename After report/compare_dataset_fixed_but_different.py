import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
"""
from subprocess import PIPE, Popen
con = sl.connect("Lund_GRIDFTP_all_fixed_delete_all.db", check_same_thread=False)

datasets_fixed=[]
#Retrive all tables from the database
all_batches=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
all_batches=[a[0] for a in all_batches]
for row in tqdm(all_batches):
    #If thea table is the table of tables, skip it
    if row == 'sqlite_sequence':
        pass
    else:
        datasets_fixed.extend(con.execute('SELECT Scope FROM '+row).fetchall())

conn=sl.connect("Lund_GRIDFTP_all_fixed_delete_all (1).db", check_same_thread=False)

datasets_fixed_but_different=[]
#Retrive all tables from the database
all_batches2=conn.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
all_batches2=[a[0] for a in all_batches2]
for row in tqdm(all_batches2):
    #If thea table is the table of tables, skip it
    if row == 'sqlite_sequence':
        pass
    else:
        datasets_fixed_but_different.extend(conn.execute('SELECT Scope FROM '+row).fetchall())

#Compare the two lists
#print("Datasets fixed but different: "+str(set(datasets_fixed_but_different)-set(datasets_fixed)))
print(len(datasets_fixed))
print(len(datasets_fixed_but_different))
#Compare the two lists
print("Files fixed but different: "+str(set(datasets_fixed)-set(datasets_fixed_but_different)))

"""
import pickle
metadata_fixed={}
# Open the file in binary mode
with open('C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\After report\\FROMLUNARC\\my_dict.pickle', 'rb') as file:

    # Use pickle to load the dictionary from the file
    total_fixed = pickle.load(file)


con=sl.connect("Lund_GRIDFTP_all_fixed_delete_all.db", check_same_thread=False)

datasets_and_files={}
for row in tqdm((con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall())):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        
        result=con.execute("SELECT file FROM {} where duplicate>0".format(row[0])).fetchall()
        result=[a[0] for a in result]
        datasets_and_files[row[0]]=result


comp_dif=[]
walltimedif=[]
LDMXIMAGE=[]
for key in tqdm(total_fixed):
    if "ComputingElement"  not in [a[0] for a in total_fixed[key]]:
        comp_dif.append(key)
    if "Walltime" not in  [a[0] for a in total_fixed[key]]:
        walltimedif.append(key)
    if "LdmxImage" in [a[0] for a in total_fixed[key]]:
        LDMXIMAGE.append(key)
print("missing")
print(len(comp_dif))
print(len(walltimedif))

print("Extra")
print(len(LDMXIMAGE))



dataset_walltime={}
for file in LDMXIMAGE:
    for key in datasets_and_files:
        if file in datasets_and_files[key]:
            if key in dataset_walltime:
                dataset_walltime[key].append(file)
            else:
                dataset_walltime[key]=[file]
print("\n\n\n")
print("walltime: ")
printstat=[]
for key in dataset_walltime:
    printstat.append((key,len(dataset_walltime[key])))


    #print("\n\n")

#order the list by the zero element of the tuple, which is the dataset name as a string
printstat=sorted(printstat, key=lambda x: x[0])





for dataset in printstat:
    f=open("C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\After report\\duplicates_{}.txt".format(dataset[0]),"r")
    result=f.read()
    result=result.split("\n")
    #remove the last empty element
    result=result[:-1]
    result=[a.split("_")[-2].replace("run","") for a in result]
    nr=len(set(result))

    print(dataset[0]+" &"+str(dataset[1])+" &"+str(round(dataset[1]*100/nr,1))+" \\%"+" \\\\ \\hline")

    #print(row+" &"+str(dataset[1])+" &"+str(int(dataset[1])/result[0][0])+" \\\\ \\hline")

"""
dataset_walltime={}
for file in walltimedif:
    for key in datasets_and_files:
        if file in datasets_and_files[key]:
            if key in dataset_walltime:
                dataset_walltime[key].append(file)
            else:
                dataset_walltime[key]=[file]

print("walltime: ")
for key in dataset_walltime:
    print(key)
    print(len(dataset_walltime[key]))
    print(dataset_walltime[key][0])
    print("\n\n")

dataset_LdmxImage={}
for file in LDMXIMAGE:
    for key in datasets_and_files:
        if file in datasets_and_files[key]:
            if key in dataset_LdmxImage:
                dataset_LdmxImage[key].append(file)
            else:
                dataset_LdmxImage[key]=[file]

                    
print("LdmxImage: ")
for key in dataset_LdmxImage:

    print(key)
    print(len(dataset_LdmxImage[key]))
    print(dataset_LdmxImage[key][0])
    print("\n\n")"""