import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import statistics
from statistics import mode

location="All_LUND"

files_in_both_file="{}/files_found_storage.txt".format(location)
files_in_both=open(files_in_both_file,"r")
files_in_both_lines=files_in_both.readlines()
files_in_both_lines=[x.replace("\n","") for x in files_in_both_lines]
files_in_both_lines=[x.split(",") for x in files_in_both_lines]
directories_in_all_files={}
for line in files_in_both_lines:
    file=line[0]
    scope=line[1]
    dataset=line[2]
    directory=line[3]
    if directory not in directories_in_all_files:
        directories_in_all_files[directory]=[]
    directories_in_all_files[directory].append([file,scope,dataset])




files_missing_rucio_file="{}/files_missing_rucio.txt".format(location)
files_missing_rucio=open(files_missing_rucio_file,"r")
files_missing_rucio_lines=files_missing_rucio.readlines()

file_missing_rucio_from_directory_name=[]

files_missing_rucio_not_from_directory_name=[]

for line in files_missing_rucio_lines:
    lione_list=line.split(",")
    file=lione_list[0]
    scope=lione_list[1]
    dataset=lione_list[2]
    directory=lione_list[3]
    directory=directory.replace("\n","")
    possible_scopes=['mc20','mc-test20','mock','test-v2.1','validation-v2.1.0','test-v2.2','validation','test','mc21','vaidation']


    #check if any of the possible scopes is a part of the directory
    if any(x in directory for x in possible_scopes):
        for scope_of in possible_scopes:
            if scope_of in directory:

                dataset=dataset.replace("None:",scope_of+":")
                file_missing_rucio_from_directory_name.append([file,scope_of,dataset,directory])
                break
    else:
        files_missing_rucio_not_from_directory_name.append([file,scope,dataset,directory])

            

directory_and_data={}
#print(files_missing_rucio_not_from_directory_name[:10])
for line in files_missing_rucio_not_from_directory_name:
    file=line[0]
    scope=line[1]
    dataset=line[2]
    directory=line[3]
    if directory in directory_and_data:
        directory_and_data[directory].append([file,scope,dataset,directory])
    else:
        directory_and_data[directory]=[[file,scope,dataset,directory]]

file_missing_rucio_from_all_files=[]
for directory in directory_and_data:
    if directory in directories_in_all_files:
        dataset=[x[2] for x in directories_in_all_files[directory]]
        setof=set(dataset)
        dataset=mode(dataset)
        scope=dataset.split(":")[0]
        stuff_tochange=directory_and_data[directory]
        for line in stuff_tochange:
            line[1]=scope
            line[2]=dataset
            file_missing_rucio_from_all_files.append(line)

print("Total number of files missing from rucio: {}".format(len(files_missing_rucio_lines)))
print("Total number of files missing from rucio and found in directory: {}".format(len(file_missing_rucio_from_directory_name)))
print("Total number of files missing from rucio and not found in directory: {}".format(len(file_missing_rucio_from_all_files)))
print("The difference between the total and the rest is: {}".format(len(files_missing_rucio_lines)-len(file_missing_rucio_from_directory_name)-len(file_missing_rucio_from_all_files)))


os.remove(files_missing_rucio_file)

with open(files_missing_rucio_file, 'w') as f:
    for item in file_missing_rucio_from_directory_name:
        f.write("%s,%s,%s,%s\n" % (item[0],item[1],item[2],item[3]))
    for item in file_missing_rucio_from_all_files:
        f.write("%s,%s,%s,%s\n" % (item[0],item[1],item[2],item[3]))