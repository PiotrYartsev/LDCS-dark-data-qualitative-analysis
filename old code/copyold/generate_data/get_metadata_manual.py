
import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen
con = sl.connect('/home/pioyar/rucio-client-venv/summer_project/duplicate_data.db')


scope="validation"
location="/projects/hep/fs9/shared/ldmx/ldcs/output/ldmx/mc-data/validation/v12/4.0GeV"


def runner(file):
    p = Popen("rucio get-metadata {}:{}".format(scope,file), shell=True, stdout=PIPE, stderr=PIPE)
    L_1, stderr = p.communicate()
    #print(type(L_1))
    #print(L)
    L=L_1.decode("utf-8").split("\n")        
    #print(stderr)
    
    if len(L)<2:
        FileCreationTime=file.split("_")[-1].replace("t","")
        FileCreationTime=FileCreationTime.replace(".roo","")
        #print(FileCreationTime)
        data.append((len(data)+1,file,None,None,None,scope,None,FileCreationTime)) 
    else:
        #print(len(L))
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

        
        data.append((len(data)+1,file,BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime)) 



for file1 in os.listdir(location):
    print(file1)
    if os.path.isdir(location+"/{}".format(file1)):
        data=[]
        file12=file1.replace('.','')
        file12=file12.replace('_','')
        file12=file12.replace('-','')
        #print(file12)
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
                    FileCreationTime INTEGER
                );
            """.format(file12))


        sql = 'INSERT INTO {} (id, file, BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime) values(?, ?, ?, ?, ?, ?, ?, ?)'.format(file12)
        files=os.listdir(location+"/{}".format(file1))
        files_1=files[:len(files)//8]
        files_2=files[len(files)//8:len(files)*2//8]
        files_3=files[len(files)*2//8:len(files)*3//8]
        files_4=files[len(files)*3//8:len(files)*4//8]
        files_5=files[len(files)*4//8:len(files)*5//8]
        files_6=files[len(files)*5//8:len(files)*6//8]
        files_7=files[len(files)*6//8:len(files)*7//8]
        files_8=files[len(files)*7//8:]


        #print(len(files))

        #print(len(files_1)+len(files_2)+len(files_3)+len(files_4)+len(files_5)+len(files_6)+len(files_7)+len(files_8))
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

        with con:
                con.executemany(sql, data)