import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 

from subprocess import PIPE, Popen
delete_all_name='Lund_GRIDFTP_all_fixed_delete_all.db'
delete_all = sl.connect('{}'.format(delete_all_name))

"""
list_of_lists=[]

for row in (delete_all.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
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
                data_location=delete_all.execute("SELECT file,Scope,ComputingElement,duplicate from {} where file_number={} and ComputingElement is not Null;".format(row[0],file_number)).fetchall()
                #print(data_location)
                if len(data_location)>1:
                    list_of_lists.append(data_location)
n=0
for i in list_of_lists:
    n=n+len(i)
print(n)
"""
"""
def runner(input):
    for input2 in input:
        scope=input2[1].replace(' ','')
        file=input2[0].replace(' ','')
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
files_to_sheck=list_of_lists
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


data={}

for key in compare_dict2:
    if len(compare_dict2[key])>1:
        if key in data:
            data[key]+=len(compare_dict2[key])
        else:
            data[key]=len(compare_dict2[key])

print(data)"""


data={'ARCCEJobID': 53137, 'BatchID': 22, 'BeamEnergy': 2, 'ComputingElement': 4, 'DataLocation': 53137, 'DetectorVersion': 2, 'ElectronNumber': 2, 'FileCreationTime': 39063, 'Geant4BiasFactor': 2, 'InputFile': 53137, 'IsTriggerSkim': 2, 'JobSubmissionTime': 656, 'LdmxImage': 3, 'MomentumVectorX': 2, 'MomentumVectorY': 2, 'MomentumVectorZ': 2, 'PhysicsProcess': 2, 'RandomSeed1': 50521, 'RandomSeed2': 53137, 'RunNumber': 1636, 'SampleId': 2, 'Scope': 2, 'Walltime': 902, 'PileupFile': 5093}

import matplotlib.pyplot as plt
name=delete_all_name.replace(".db","")
name2=name.replace("_all","")
name2=name.replace("_2","")
name2=name2.replace("_"," ")


plt.bar(range(len(data)), list(data.values()), align='center')
#if the value is less than 500 write it above the bar
for i, v in enumerate(data.values()):
    if v<500:
        plt.text(i-0.2, v+100, str(v), color='blue', fontweight='bold')
#make y grid
plt.grid(axis='y', alpha=0.75)
plt.xticks(range(len(data)), list(data.keys()),rotation=90)
plt.ylabel('Number of files')
plt.title('{}: Number of files with different metadata'.format(name2))
#change the size of the plot
plt.rcParams["figure.figsize"] = (20,10)
plt.tight_layout(rect=[0,0.03,1,1])
name2=name2.replace(" ","_")
plt.show()