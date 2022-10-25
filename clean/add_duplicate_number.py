from itertools import count
from logging import raiseExceptions
from multiprocessing.connection import wait
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
import time
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen


from sqlalchemy import column
def add_duplicate_number(dataset):

    if not os.path.isfile(dataset[:-3]+".txt"):
        f = open(dataset[:-3]+".txt", 'w')


    already_checked=open(dataset[:-3]+".txt","r").read().split("\n")
    already_checked=[a for a in already_checked if a != ""]

    con = sl.connect(dataset,isolation_level=None)
    with con:
        #con.isolation_level = None
        row2s=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    
    for row2 in row2s:
        
        row=row2[0]
        if row in already_checked:
            print("Already checked "+row)
            pass
        else:
            
            if row == 'sqlite_sequence':
                pass
            else:
                print("          "+row)

                try:
                    with con:
                        con.execute("Select duplicate from %s" % row).fetchall()

                except:



                    
                    with con:
                        any_duplicates=con.execute("Select file_number from %s" % row).fetchall()

                    
                    position=list(set(any_duplicates))
                    position=[a[0] for a in position]
                    if len(any_duplicates)==len(list(set(any_duplicates))):
                        print("          "+"No duplicates\n\n")
                        with con:
                            con.execute("ALTER TABLE {} ADD duplicate INTEGER;".format(row))
                        con.execute("UPDATE {} SET duplicate = NULL;".format(row))
                    else:
                        

                        print("          "+"Duplicates")

                        
                        

                        time_1={}
                        
                        
                            
                        def get_data(number2,row):
                            import time
                            column="id"
                            

                            with con:
                                creation_time = con.execute("SELECT FileCreationTime,{} FROM {} WHERE file_number = ?;".format(column,row), (int(number2),)).fetchall()

                            if len(creation_time)==0:
                                pass
                            else:
                                time=[]
                                number=[]
                                if len(creation_time)>1:
                                    for i in range(len(creation_time)):
                                        time.append(creation_time[i][0])
                                        number.append(creation_time[i][1])
                                    Z=[x for _, x in sorted(zip(time, number))]

                                    time_1[str(number2)]=Z

                                    
                                else:
                                    pass
                        files=position
                        data=[]
                        for number2 in tqdm(files):
                            if number2==None:
                                pass
                            else:
                                get_data(number2,row)

            
                        time_1_list=time_1
                        with con:
                            con.execute("ALTER TABLE {} ADD duplicate INTEGER;".format(row))
                        print("          "+"Update database")

                        data=[]
                        sql="UPDATE {} SET duplicate = ? WHERE id = ?;".format(row)

                        with con:
                            for i in tqdm(time_1_list):
                                lists=time_1_list[i]
                                #print(i,lists)
                                if not i.isnumeric():
                                    print("not numeric")
                                
                                for n in range(len(lists)):
                                    data.append((n+1,lists[n]))
                        con.execute("Begin transaction;")
                        con.executemany(sql,data)
                        con.execute("Commit;")

                        print("          "+"\n")
                f=open(dataset[:-3]+".txt", 'a')
                f.write(row+"\n")           
                f.close()

    #delete the file
    os.remove(dataset[:-3]+".txt")