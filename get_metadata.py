import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl

con = sl.connect('/home/pioyar/rucio-client-venv/summer_project/duplicate_data.db')

"""
with con:
    con.execute("""
        CREATE TABLE USER (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            file TEXT,
            BatchID TEXT,
            ComputingElement TEXT,
            DataLocation TEXT,
            Scope TEXT,
            JobSubmissionTime INTEGER,
            FileCreationTime INTEGER
        );
    """)
"""

sql = 'INSERT INTO USER (id, file, BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime) values(?, ?, ?, ?, ?, ?, ?, ?)'
data = [
    (1, 'Alice', 21),
    (2, 'Bob', 22),
    (3, 'Chris', 23)
]


data=[]
for file in os.listdir("/projects/hep/fs9/shared/ldmx/ldcs/gridftp/mc20/v9/4.0GeV/v1.7.1_ecal_photonuclear_reco_bdt-batch1"):
    
    L=(os.popen("rucio get-metadata mc20:{}".format(file)).read()).split("\n")
    
    L2=[]
    for n in range(len(L)):
        a=L[n]
        a2=a.split(":")
        if a2==['']:
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
                JobSubmissionTime=a2[1]
            if "FileCreationTime" in a2[0]:
                FileCreationTime=a2[1]

        L2.append(a2)    
    data.append((len(data)+1,file,BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime))    

    


with con:
    con.executemany(sql, data)
