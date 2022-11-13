from logging import raiseExceptions
from tokenize import Number

from tqdm import *

from subprocess import PIPE, Popen

from zlib import adler32
import sqlite3 as sl

from subprocess import PIPE, Popen




def fix_many_batches_in_one(dataset):
    con = sl.connect(dataset, check_same_thread=False)

    #Retrive all tables from the database
    all_batches=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    all_batches=[a[0] for a in all_batches]
    for row in all_batches:
        #If thea table is the table of tables, skip it
        if row == 'sqlite_sequence':
            pass
        else:
            data={}
            None_list=[]
            print("          "+row+"\n")
            #Retrive all the batches from the table
            batches=con.execute("Select BatchId from %s" % row).fetchall()
            batches=list(set(batches))
            batches=[a[0] for a in batches]
            

            if None in batches:
                batches.remove(None)
           
            
            #sheck if there is more then one batch

            if len(batches)>1:
                #Retrive all the data from the table
                stuff_to_add_to_new_batch=con.execute("Select file,BatchID,ComputingElement,DataLocation,Scope,FileCreationTime,IsRecon,JobSubmissionTime from {};".format(row)).fetchall()
                print("          "+"More than one batch")
                
                for batch in batches:
                    for n in range(len(stuff_to_add_to_new_batch)):
                        if batch in stuff_to_add_to_new_batch[n]:
                            if batch in data:
                                data[batch].append(stuff_to_add_to_new_batch[n])
                            else:
                                data[batch]=[stuff_to_add_to_new_batch[n]]
                BactH_is_none=con.execute("Select file,BatchID,ComputingElement,DataLocation,Scope,FileCreationTime,IsRecon,JobSubmissionTime from {} where BatchID = Null;".format(row)).fetchall()
                
                for n in range(len(BactH_is_none)):
                        None_list.append(BactH_is_none[n])
                        
                filename_for_batch={}

                for batch2 in data:

                    data2=data[batch2]
                    batch2=batch2.replace(' ','')
                    batch2=batch2.replace('.','')
                    batch2=batch2.replace('_','')
                    batch2=batch2.replace('-','')
                    if batch2[0].isnumeric():
                        batch2="A"+batch2
                    
                    if batch2.lower()==row:
                        
                        batch2="new_"+batch2
                    print("                    "+"Sub-batch: {}".format(batch2))
                    all_batches2=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
                    all_batches2=[a[0].lower() for a in all_batches2]
                    
                    if batch2.lower() in all_batches2:
                        print("                    "+"Table already exists")
                        print("                    "+"Making another version")
                        max_id=con.execute("Select MAX(id) from {}".format(batch2)).fetchone()[0]
                        if max_id is None:
                            max_id=0
                        
                    else:
                        print("                    "+"Table does not exist")
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
                    test={}
                    for n in range(len(data2)):
                        if data2[n][1] in test:
                            test[data2[n][0]].append(n)
                        else:
                            test[data2[n][0]]=[n]
                    #print(len(data3))
                    data_test=[]     
                    for key in test:
                        #print(key)
                        #print(data2[test[key]])
                        data_test.append(data2[test[key][0]])
                    
                    data3=[]
                    if len(data2)!=len(data_test):
                        print("                    "+str(len(test)))
                        print("                    "+"Different number of files",len(data2)," vs ",len(data_test))
                        
                    for a in range(len(data_test)):
                        data3.append((max_id+a+1,data_test[a][0],data_test[a][1],data_test[a][2],data_test[a][3],data_test[a][4],data_test[a][5],data_test[a][6],data_test[a][7]))

                    sql = 'INSERT INTO {} (id, file, BatchID,ComputingElement,DataLocation,Scope,FileCreationTime,IsRecon,JobSubmissionTime) values(?, ?, ?, ?, ?, ?, ?, ?, ?)'.format(batch2)
                    print("                    "+"Wrtiting to table {}".format(batch2))
                    with con:
                        con.executemany(sql, data3)

                    print("                    "+"Done\n")
                print("                    "+"Deleting old table {}".format(row))
                con.execute("DROP TABLE {}".format(row))
                print("                    "+"\n")
            else:
                pass
                print("          "+"No extra batches\n")
        