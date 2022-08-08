from itertools import count
from logging import raiseExceptions
from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from tqdm import *
import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen
import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen


from sqlalchemy import column



def fix_many_batches_in_one(dataset):
    con = sl.connect(dataset, check_same_thread=False)
    all_batches=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    all_batches=[a[0] for a in all_batches]
    for row in all_batches:
        if row == 'sqlite_sequence':
            pass
        else:
            data={}
            print("          "+row)
            batches=con.execute("Select BatchId from %s" % row).fetchall()
            batches=list(set(batches))
            batches=[a[0] for a in batches]
            
            if None in batches:
                batches.remove(None)
            #print("          "+batches)
            stuff_to_add_to_new_batch=con.execute("Select file,BatchID,ComputingElement,DataLocation,Scope,FileCreationTime,IsRecon,JobSubmissionTime from {};".format(row)).fetchall()
            if len(batches)>1:
                print("          "+"More than one batch")
                #con.execute("DELETE FROM {} WHERE BatchId = Null;")
                for batch in batches:
                    #stuff_to_add_to_new_batch=con.execute("Select * from {} where BatchId = {};".format(row,batch)).fetchall()
                    
                    for n in range(len(stuff_to_add_to_new_batch)):
                        if batch in stuff_to_add_to_new_batch[n]:
                            if batch in data:
                                data[batch].append(stuff_to_add_to_new_batch[n])
                            else:
                                data[batch]=[stuff_to_add_to_new_batch[n]]
                for batch2 in data:
                    data2=data[batch2]
                    batch2=batch2.replace(' ','')
                    batch2=batch2.replace('.','')
                    batch2=batch2.replace('_','')
                    batch2=batch2.replace('-','')
                    #print("          "+batch2[0])
                    if batch2[0].isnumeric():
                        #print("          "+"True")
                        batch2="A"+batch2
                    print("          "+"Sub-batch: {}".format(batch2))
                    #print("          "+batch2)
                    
                    if batch2 in all_batches:
                        print("          "+"Table already exists")
                        max_id=con.execute("Select MAX(id) from {}".format(batch2)).fetchone()[0]
                        if max_id is None:
                            max_id=0
                        
                    else:
                        print("          "+"Table does not exist")
                        max_id=0
                        con.execute("""
                            CREATE TABLE {} (
                                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                file TEXT,
                                BatchID TEXT,
                                ComputingElement TEXT,
                                DataLocation TEXT,
                                Scope TEXT,
                                FileCreationTime INTEGER,
                                IsRecon TEXT,
                                JobSubmissionTime INTEGER
                            );
                        """.format(batch2))
                    data3=[]
                    for a in range(len(data2)):
                        #print("          "+(data2[a]))
                        data3.append((max_id+a+1,data2[a][0],data2[a][1],data2[a][2],data2[a][3],data2[a][4],data2[a][5],data2[a][6],data2[a][7]))
                    data2=data3
                    #print("          "+data2[0])
                    sql = 'INSERT INTO {} (id, file, BatchID,ComputingElement,DataLocation,Scope,FileCreationTime,IsRecon,JobSubmissionTime) values(?, ?, ?, ?, ?, ?, ?, ?, ?)'.format(batch2)
                    
                    
                    print("          "+"Wrtiting to table {}".format(batch2))
                    with con:
                        con.executemany(sql, data2)
                    print("          "+"Done")
                print("          "+"Deleting old table {}".format(row))
                con.execute("DROP TABLE {}".format(row))
                print("          "+"\n\n")
            else:
                print("          "+"No extra batches\n\n")