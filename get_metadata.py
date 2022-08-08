import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl


con = sl.connect('/home/pioyar/rucio-client-venv/summer_project/duplicate_data.db')




scope="mc20"
location="/projects/hep/fs9/shared/ldmx/ldcs/output/ldmx/mc-data/mc20/v12/4.0GeV"

for file1 in os.listdir(location):
    print(file1)
    if os.path.isdir(location+"/{}".format(file1)):
        data=[]
        file12=file1.replace('.','')
        file12=file12.replace('_','')
        file12=file12.replace('-','')
        print(file12)
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
        for file in tqdm(os.listdir(location+"/{}".format(file1))):
            

            
            L=(os.popen("rucio get-metadata {}:{}".format(scope,file)).read()).split("\n")
            
            if len(L)<2:
                print(L)
                pass
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

            


        with con:
            con.executemany(sql, data)