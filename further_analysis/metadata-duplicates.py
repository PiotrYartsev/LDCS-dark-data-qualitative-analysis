import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 

from subprocess import PIPE, Popen
delete_all_name='Lund_GRIDFTP_all_fixed_delete_all.db'
delete_all = sl.connect('{}'.format(delete_all_name))




stuff={}


for row in tqdm((delete_all.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall())):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        file_number_dup_more_then2=delete_all.execute("SELECT file_number from {} where duplicate>2;".format(row[0])).fetchall()

        print(row[0])
        if len(file_number_dup_more_then2)>0:
            file_number_dup_more_then2=[i[0] for i in file_number_dup_more_then2]

            file_number_dup_more_then2=list(set(file_number_dup_more_then2))

            #get the DataLocations for each file_number
            for file_number in file_number_dup_more_then2:
                data_location=delete_all.execute("SELECT file,Scope from {} where file_number={} and ComputingElement is not null;".format(row[0],file_number)).fetchall()
                if len(data_location)>1:
                    if row[0] in stuff:
                        stuff[row[0]].append(data_location)
                    else:
                        stuff[row[0]]=[data_location]


def runner(input):
    scope=input[1].replace(' ','')
    file=input[0].replace(' ','')
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









            
compare_dict2={}






for thing in stuff:
    print(thing)
    for i in stuff[thing]:
        for n in i:
            runner(n)
        break
    break

print(compare_dict2)

                            