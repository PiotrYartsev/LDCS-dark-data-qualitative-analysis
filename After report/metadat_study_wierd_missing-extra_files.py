import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 

from subprocess import PIPE, Popen



def runner(input):
    for input2 in input:
        scope=input2[0]
        file=input2[1]
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


#open file C:\Users\piotr\Documents\GitHub\LDCS-dark-data-qualitative-analysis\list_of_duplicates.txt

file_to_read=open('list_of_duplicates.txt','r')
list_of_lists=[]
n=0
while n>10:
    n+=1
    for line in file_to_read:
        line=line.split(',')
        line=line[:-1]
        list_of_lists.append(line)
file_to_read.close()

list_of_lists=[[a.split(':') for a in b] for b in list_of_lists]
print(list_of_lists[0])

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

print(data)
#"""