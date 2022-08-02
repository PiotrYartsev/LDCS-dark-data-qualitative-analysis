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


from sqlalchemy import column
position_ult=[]



con = sl.connect('Lund_all.db', check_same_thread=False)
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        try:
            con.execute("Select duplicate from %s" % row[0]).fetchall()
        except:
            #max_file_number = con.execute("SELECT MAX(file_number) FROM {};".format(row[0])).fetchone()[0]
            #min_file_number = con.execute("SELECT MIN(file_number) FROM {};".format(row[0])).fetchone()[0]
            #print(max_file_number)
            #position=list(range(min_file_number,max_file_number+1))
            
            
            

            
            any_duplicates=con.execute("Select file_number from %s" % row[0]).fetchall()
            
            position=list(set(any_duplicates))
            position=[a[0] for a in position]
            if len(any_duplicates)==len(list(set(any_duplicates))):
                print("No duplicates\n\n")
                with con:
                    con.execute("""
                    ALTER TABLE {}
                        ADD duplicate INTEGER;""".format(row[0]))
                con.execute("""
                UPDATE {}
                    SET duplicate = NULL;""".format(row[0]))
            else:
                

                #print(len(position))
                print("Duplicates")

                
                

                time_1={}
                column="id"
                
                    
                def get_data(number2):
                    #print(int(number))
                    creation_time = con.execute("SELECT FileCreationTime,{} FROM {} WHERE file_number LIKE ?;".format(column,row[0]), (int(number2),)).fetchall()
                    #print(creation_time)
                    if len(creation_time)==0:
                        pass
                    else:
                        time=[]
                        number=[]
                        if len(creation_time)>1:
                        # print(creation_time)
                            for i in range(len(creation_time)):
                                time.append(creation_time[i][0])
                                number.append(creation_time[i][1])
                            Z=[x for _, x in sorted(zip(time, number))]
                            #print(Z)

                            time_1[str(number2)]=Z

                            
                        else:
                            pass
                files=position
                files_1=files[:len(files)//3]
                files_2=files[len(files)//3:len(files)*2//3]
                files_3=files[len(files)*2//3:]

                #print(files_1)

                #print(len(files))

                #print(len(files_1)+len(files_2)+len(files_3))

                with tqdm(total=max(len(files_1),len(files_2),len(files_3))) as pbar:
                    data=[]
                    for (a1,a2,a3) in itertools.zip_longest(files_1,files_2,files_3):
                        #print(len(data))
                        #print(a)
                        if not a1==None:
                            t1 = threading.Thread(target=get_data, args=(a1,))
                        if not a2==None:
                            t2 = threading.Thread(target=get_data, args=(a2,))
                        if not a3==None:
                            t3 = threading.Thread(target=get_data, args=(a3,))



                        if not a1==None:
                            t1.start()
                        if not a2==None:
                            t2.start()
                        if not a3==None:
                            t3.start()






                            
                        if not a1==None:
                            t1.join()
                        if not a2==None:
                            t2.join()
                        if not a3==None:
                            t3.join()


                        pbar.update(1)

        
                time_1_list=time_1

            
    

                with con:
                    con.execute("ALTER TABLE {} ADD duplicate INTEGER;".format(row[0]))
                print("Update database")
                with con:
                    for i in tqdm(time_1_list):
                        lists=time_1_list[i]
                        #print(i+"\n")
                        for n in lists:
                            
                            #print(n)
                            
                            
                            con.execute("UPDATE {} SET duplicate = {} WHERE id LIKE {};".format(row[0],i,n))
                            
                print("\n\n")