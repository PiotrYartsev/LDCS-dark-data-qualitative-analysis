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
            #remove duplicates
            batches=list(set(batches))
            batches=[a[0] for a in batches]
            
            #to solve an old bug, but left in case it happens again
            if None in batches:
                batches.remove(None)
           
            
            #sheck if there is more then one batch
            if len(batches)>1:
                #Retrive all the data from the table
                stuff_to_add_to_new_batch=con.execute("Select file,BatchID,ComputingElement,DataLocation,Scope,FileCreationTime,IsRecon,JobSubmissionTime from {};".format(row)).fetchall()
                print("          "+"More than one batch")
                
                #Make a dictionary with the batch as key and the data as value
                for batch in batches:
                    for n in range(len(stuff_to_add_to_new_batch)):
                        if batch in stuff_to_add_to_new_batch[n]:
                            if batch in data:
                                data[batch].append(stuff_to_add_to_new_batch[n])
                            else:
                                data[batch]=[stuff_to_add_to_new_batch[n]]
                #Make a new batch for each batch in the dictionary
                for batch2 in data:
                    #Remove spaces, dots, underscores and dashes from the batch name
                    data2=data[batch2]
                    batch2=batch2.replace(' ','')
                    batch2=batch2.replace('.','')
                    batch2=batch2.replace('_','')
                    batch2=batch2.replace('-','')
                    #If the batch name starts with a number, add a letter in front of it as SQLite does not allow table names to start with a number
                    if batch2[0].isnumeric():
                        batch2="A"+batch2
                    
                    #Check if one of the batches has the same name as the table
                    if batch2.lower()==row:
                        batch2="new_"+batch2
                    
                    print("                    "+"Sub-batch: {}".format(batch2))
                    #Check if the batch already exists
                    all_batches2=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
                    all_batches2=[a[0].lower() for a in all_batches2]
                    
                    #If the batch already exists, we add the data to the existing batch
                    if batch2.lower() in all_batches2:
                        print("                    "+"Table already exists")
                        print("                    "+"Adding data to table {}".format(batch2))
                        max_id=con.execute("Select MAX(id) from {}".format(batch2)).fetchone()[0]
                        if max_id is None:
                            max_id=0
                    #If the batch does not exist, we create it
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

                    #Add the data to the batch

                    #honestly dont know why I did this, but it works
                    test={}
                    for n in range(len(data2)):
                        if data2[n][1] in test:
                            test[data2[n][0]].append(n)
                        else:
                            test[data2[n][0]]=[n]
                    data_test=[]     
                    for key in test:
                        data_test.append(data2[test[key][0]])
                    
                    data3=[]
                    if len(data2)!=len(data_test):
                        print("                    "+str(len(test)))
                        print("                    "+"Different number of files",len(data2)," vs ",len(data_test))
                    #defining the data to be added to the new batch                    
                    for a in range(len(data_test)):
                        data3.append((max_id+a+1,data_test[a][0],data_test[a][1],data_test[a][2],data_test[a][3],data_test[a][4],data_test[a][5],data_test[a][6],data_test[a][7]))
                    #Adding the data to the new batch
                    sql = 'INSERT INTO {} (id, file, BatchID,ComputingElement,DataLocation,Scope,FileCreationTime,IsRecon,JobSubmissionTime) values(?, ?, ?, ?, ?, ?, ?, ?, ?)'.format(batch2)
                    print("                    "+"Wrtiting to table {}".format(batch2))
                    with con:
                        con.executemany(sql, data3)

                    print("                    "+"Done\n")
                #Delete the old table, as it is no longer needed
                print("                    "+"Deleting old table {}".format(row))
                con.execute("DROP TABLE {}".format(row))
                print("                    "+"\n")
            else:
                pass
                print("          "+"No extra batches\n")
        