
import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen
con = sl.connect('/home/pioyar/rucio-client-venv/summer_project/Lund_GRID_all.db')





def runner(scope,file,max_number):
    p = Popen("rucio get-metadata {}:{}".format(scope,file), shell=True, stdout=PIPE, stderr=PIPE)
    L_1, stderr = p.communicate()
    #print(type(L_1))
    stderr=stderr.decode("utf-8").split("\n")

    L=L_1.decode("utf-8").split("\n")  
    if len(stderr)>1:
        pass    
        #print(stderr)
    
    if len(L)<2:
        FileCreationTime=file.split("_")[-1].replace("t","")
        FileCreationTime=FileCreationTime.replace(".roo","")
        #print(FileCreationTime)
        data.append((len(data)+1+max_number,file,None,None,None,scope,None,FileCreationTime,None)) 
    else:
        
        for n in range(len(L)):
            a=L[n]
            
            a2=a.split(":")
            #print(a2)
            if a2==['']:
                pass
            else:
                a2[1]=a2[1].replace('   ','')
                if "BatchID" in a2[0]:
                    BatchID=a2[1]
                    #print(BatchID)
                if "ComputingElement" in a2[0]:
                    ComputingElement=a2[1]
                if "DataLocation" in a2[0]:
                    DataLocation=a2[1]
                if "Scope" in a2[0]:
                    Scope=a2[1]
                if "JobSubmissionTime" in a2[0]:
                    JobSubmissionTime=str(a2[1])+':'+str(a2[2])+':'+str(a2[3])
                    #print(JobSubmissionTime)
                if "FileCreationTime" in a2[0]:
                    FileCreationTime=a2[1]
                if "IsRecon" in a2[0]:
                    IsRecon=a2[1]
        
        data.append((len(data)+1+max_number,file,BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime,IsRecon)) 

datasets=open("/home/pioyar/rucio-client-venv/project/output/All_LUND_2022-07-30_22:43:22.053695/files_missing_rucio.txt","r")
lines=datasets.readlines()

datasets_and_data={}
for line in lines:
    #print(line)
    filestuff=line.split(",")
    filename=filestuff[0]
    scope=filestuff[1]
    #print(scope)
    #print(filename)
    try:
        dataset=filestuff[1].split(":")[1]
        dataset=dataset.replace("\n","")
    except:
        dataset=filename.split("_")
        #print(dataset)
        dataset=dataset[:-2]
        string=""
        for n in dataset:
            string=string+str(n)
        dataset=string
        
    if dataset[:2]=="mc":
        dataset=dataset[2:]
    #dataset=dataset.removeprefix("mc")
    #print(dataset)
    
    file12=dataset.replace('.','')
    file12=file12.replace('_','')
    file12=file12.replace('-','')
    #print(file12)
    if file12[0].isnumeric():
        #print("True")
        file12="A"+file12
    if file12 not in datasets_and_data:
        datasets_and_data[file12]=[]
        datasets_and_data[file12].append([filename,scope,dataset])
    else:
        datasets_and_data[file12].append([filename,scope,dataset])
    
"""
for a in datasets_and_data:
    print(a)
    print(len(datasets_and_data[a]))"""

for dataset_2 in datasets_and_data:
    
    print(dataset_2)
    #print(len(datasets_and_data[dataset_2]))
    tables=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    tables=[h[0] for h in tables]
    #print(tables)
    if dataset_2 in tables:
        print("Table already exist")
        max_number=con.execute("SELECT MAX(id) FROM {};".format(dataset_2)).fetchall()
        max_number=max_number[0][0]
        
        print("Starting id is:"+str(max_number))
    else:
        print("Making new table")
        max_number=0
        with con:
            con.execute("""
                CREATE TABLE {} (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    file TEXT,
                    BatchID TEXT,
                    ComputingElement TEXT,
                    DataLocation TEXT,
                    Scope TEXT,
                    JobSubmissionTime INTEGER,
                    FileCreationTime INTEGER,
                    IsRecon TEXT
                );
            """.format(dataset_2))


    sql = 'INSERT INTO {} (id, file, BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime,IsRecon) values(?, ?, ?, ?, ?, ?, ?, ?, ?)'.format(dataset_2)
    files=datasets_and_data[dataset_2]
    #print(files)
    
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
    #print(files_1)

    print(len(files))

    print(len(files_1)+len(files_2)+len(files_3)+len(files_4)+len(files_5)+len(files_6)+len(files_7)+len(files_8)+len(files_9)+len(files_10)+len(files_11)+len(files_12)+len(files_13)+len(files_14)+len(files_15)+len(files_16))
    
    with tqdm(total=max(len(files_1),len(files_2),len(files_3),len(files_4),len(files_5),len(files_6),len(files_7),len(files_8),len(files_9),len(files_10),len(files_11),len(files_12),len(files_13),len(files_14),len(files_15),len(files_16))) as pbar:
        data=[]
        for (a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16) in itertools.zip_longest(files_1,files_2,files_3,files_4,files_5,files_6,files_7,files_8,files_9,files_10,files_11,files_12,files_13,files_14,files_15,files_16):
            #print(len(data))
            #print(a)
            if not a1==None:
                t1 = threading.Thread(target=runner, args=(a1[1],a1[0],max_number,))
            if not a2==None:
                t2 = threading.Thread(target=runner, args=(a2[1],a2[0],max_number,))
            if not a3==None:
                t3 = threading.Thread(target=runner, args=(a3[1],a3[0],max_number,))
            if not a4==None:
                t4 = threading.Thread(target=runner, args=(a4[1],a4[0],max_number,))
            if not a5==None:
                t5 = threading.Thread(target=runner, args=(a5[1],a5[0],max_number,))
            if not a6==None:
                t6 = threading.Thread(target=runner, args=(a6[1],a6[0],max_number,))
            if not a7==None:
                t7 = threading.Thread(target=runner, args=(a7[1],a7[0],max_number,))
            if not a8==None:
                t8 = threading.Thread(target=runner, args=(a8[1],a8[0],max_number,))
            if not a9==None:
                t9 = threading.Thread(target=runner, args=(a9[1],a9[0],max_number,))
            if not a10==None:
                t10 = threading.Thread(target=runner, args=(a10[1],a10[0],max_number,))
            if not a11==None:
                t11 = threading.Thread(target=runner, args=(a11[1],a11[0],max_number,))
            if not a12==None:
                t12 = threading.Thread(target=runner, args=(a12[1],a12[0],max_number,))
            if not a13==None:
                t13 = threading.Thread(target=runner, args=(a13[1],a13[0],max_number,))
            if not a14==None:
                t14 = threading.Thread(target=runner, args=(a14[1],a14[0],max_number,))
            if not a15==None:
                t15 = threading.Thread(target=runner, args=(a15[1],a15[0],max_number,))
            if not a16==None:
                t16 = threading.Thread(target=runner, args=(a16[1],a16[0],max_number,))


            if not a1==None:
                t1.start()
            if not a2==None:
                t2.start()
            if not a3==None:
                t3.start()
            if not a4==None:
                t4.start()
            if not a5==None:
                t5.start()
            if not a6==None:
                t6.start()
            if not a7==None:
                t7.start()
            if not a8==None:
                t8.start()
            if not a9==None:
                t9.start()
            if not a10==None:
                t10.start()
            if not a11==None:
                t11.start()
            if not a12==None:
                t12.start()
            if not a13==None:
                t13.start()
            if not a14==None:
                t14.start()
            if not a15==None:
                t15.start()
            if not a16==None:
                t16.start()





                
            if not a1==None:
                t1.join()
            if not a2==None:
                t2.join()
            if not a3==None:
                t3.join()
            if not a4==None:
                t4.join()
            if not a5==None:
                t5.join()
            if not a6==None:
                t6.join()
            if not a7==None:
                t7.join()
            if not a8==None:
                t8.join()
            if not a9==None:
                t9.join()
            if not a10==None:
                t10.join()
            if not a11==None:
                t11.join()
            if not a12==None:
                t12.join()
            if not a13==None:
                t13.join()
            if not a14==None:
                t14.join()
            if not a15==None:
                t15.join()
            if not a16==None:
                t16.join()

            pbar.update(1)
    with con:
            con.executemany(sql, data)