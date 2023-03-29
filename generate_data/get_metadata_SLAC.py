
import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen

#the following is for the SLAC GRIDFTP
def runner(file):
    #ahve to set the scope manually for the SLAC GRIDFTP 
    scope2="mc20"
    #retrive the metadata from RuCIO for the file
    p = Popen("rucio get-metadata {}:{}".format(scope2,file), shell=True, stdout=PIPE, stderr=PIPE)
    #L_1 is the output of the command and stderr is the error
    L_1, stderr = p.communicate()
    
    L=L_1.decode("utf-8").split("\n") 

    #if the output is empty or very short, then the file is not in the database   
    if len(L)<2:    
        print(stderr.decode("utf-8").split("\n"))
    else:
        for n in range(len(L)):
            a=L[n]
            a2=a.split(":")
            if a2==['']:
                #print(a2)
                pass
            else:
                a2[1]=a2[1].replace('   ','')
                if "BatchID" in a2[0]:
                    BatchID=a2[1]
        filenam_list.append(file)
        scope.append('mc20')
        dataset.append(BatchID)



        
     

datasets=open("/home/pioyar/rucio-client-venv/project/output/All_SLAC_GRIDFTP_time/files_found_storage.txt","r")
lines=datasets.readlines()
files=[]
for line in lines:
    files.append(line)
    

    

files_1=files[:len(files)//16]
files_2=files[len(files)//16:len(files)*2//16]
files_3=files[len(files)*2//16:len(files)*3//16]
files_4=files[len(files)*3//16:len(files)*4//16]
files_5=files[len(files)*4//16:len(files)*5//16]
files_6=files[len(files)*5//16:len(files)*6//16]
files_7=files[len(files)*6//16:len(files)*7//16]
files_8=files[len(files)*7//16:len(files)*8//16]
files_9=files[len(files)*8//16:len(files)*9//16]
files_10=files[len(files)*9//16:len(files)*10//16]
files_11=files[len(files)*10//16:len(files)*11//16]
files_12=files[len(files)*11//16:len(files)*12//16]
files_13=files[len(files)*12//16:len(files)*13//16]
files_14=files[len(files)*13//16:len(files)*14//16]
files_15=files[len(files)*14//16:len(files)*15//16]
files_16=files[len(files)*15//16:]

filenam_list=[]
scope=[]
dataset=[]
print(len(files))

print(len(files_1)+len(files_2)+len(files_3)+len(files_4)+len(files_5)+len(files_6)+len(files_7)+len(files_8)+len(files_9)+len(files_10)+len(files_11)+len(files_12)+len(files_13)+len(files_14)+len(files_15)+len(files_16))



with tqdm(total=max(len(files_1),len(files_2),len(files_3),len(files_4),len(files_5),len(files_6),len(files_7),len(files_8),len(files_9),len(files_10),len(files_11),len(files_12),len(files_13),len(files_14),len(files_15),len(files_16))) as pbar:
    for (a,b,c,d,e,f,g,h, k, l,m,n,o,p,w,z) in itertools.zip_longest(files_1,files_2,files_3,files_4,files_5,files_6,files_7,files_8,files_9,files_10,files_11,files_12,files_13,files_14,files_15,files_16):
        t1 = threading.Thread(target=runner, args=(a,))
        t2 = threading.Thread(target=runner, args=(b,))
        t3 = threading.Thread(target=runner, args=(c,))
        t4 = threading.Thread(target=runner, args=(d,))
        t5 = threading.Thread(target=runner, args=(e,))
        t6 = threading.Thread(target=runner, args=(f,))
        t7 = threading.Thread(target=runner, args=(g,))
        t8 = threading.Thread(target=runner, args=(h,))
        t9 = threading.Thread(target=runner, args=(k,))
        t10 = threading.Thread(target=runner, args=(l,))
        t11 = threading.Thread(target=runner, args=(m,))
        t12 = threading.Thread(target=runner, args=(n,))
        t13 = threading.Thread(target=runner, args=(o,))
        t14 = threading.Thread(target=runner, args=(p,))
        t15 = threading.Thread(target=runner, args=(w,))
        t16 = threading.Thread(target=runner, args=(z,))

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
        if not k==None:
            t9.start()
        if not l==None:
            t10.start()
        if not m==None:
            t11.start()
        if not n==None:
            t12.start()
        if not o==None:
            t13.start()
        if not p==None:
            t14.start()
        if not w==None:
            t15.start()
        if not z==None:
            t16.start()




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
        if not k==None:
            t9.join()
        if not l==None:
            t10.join()
        if not m==None:
            t11.join()
        if not n==None:
            t12.join()
        if not o==None:
            t13.join()
        if not p==None:
            t14.join()
        if not w==None:
            t15.join()
        if not z==None:
            t16.join()



        pbar.update(1)
        


print(len(filenam_list))
print(len(scope))
print(len(dataset))
with open("/home/pioyar/rucio-client-venv/project/output/All_SLAC_GRIDFTP_time/files_found_storage2.txt","w") as f:
    for n in range(len(filenam_list)):
        filenma=filenam_list[n].replace('\n','')
        f.write(filenma+','+scope[n]+','+scope[n]+':'+dataset[n]+'\n')
    f.close()