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
    
    
    con = sl.connect(dataset)
    with con:
        #con.isolation_level = None
        row2s=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    
    for row2 in row2s:

        row=row2[0]
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
                        con.execute("""
                        ALTER TABLE {}
                            ADD duplicate INTEGER;""".format(row))
                    con.execute("""
                    UPDATE {}
                        SET duplicate = NULL;""".format(row))
                else:
                    

                    print("          "+"Duplicates")

                    
                    

                    time_1={}
                    
                    
                        
                    def get_data(number2,row):
                        column="id"
                        

                        with con:
                            creation_time = con.execute("SELECT FileCreationTime,{} FROM {} WHERE file_number LIKE ?;".format(column,row), (int(number2),)).fetchall()

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
                        get_data(number2,row)
                    """
                    files_1=files[:len(files)//3]
                    files_2=files[len(files)//3:len(files)*2//3]
                    files_3=files[len(files)*2//3:]


                    
                    with tqdm(total=max(len(files_1),len(files_2),len(files_3))) as pbar:
                        data=[]
                        for (a1,a2,a3) in itertools.zip_longest(files_1,files_2,files_3):

                            if not a1==None:
                                t1 = threading.Thread(target=get_data, args=(a1,row,))
                            if not a2==None:
                                t2 = threading.Thread(target=get_data, args=(a2,row,))
                            if not a3==None:
                                t3 = threading.Thread(target=get_data, args=(a3,row,))



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
                    """
        
                    time_1_list=time_1
                    #print(time_1_list)
                    with con:
                        con.execute("ALTER TABLE {} ADD duplicate INTEGER;".format(row))
                    print("          "+"Update database")

                    
                    with con:
                        for i in tqdm(time_1_list):
                            lists=time_1_list[i]
                            for n in lists:
                                print(row, i ,n)
                                con.execute("UPDATE {} SET duplicate = {} WHERE id LIKE {};".format(row,i,n))
                    print("          "+"\n\n")