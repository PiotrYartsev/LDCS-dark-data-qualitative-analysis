from itertools import count
from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl

from sqlalchemy import column
position_ult=[]



con = sl.connect('SLAC_mc20_2.db')
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        max_file_number = con.execute("""
        SELECT MAX(file_number) FROM {};""".format(row[0])).fetchone()[0]
        min_file_number = con.execute("""
        SELECT MIN(file_number) FROM {};""".format(row[0])).fetchone()[0]
        #print(max_file_number)
        position=list(range(min_file_number,max_file_number+1))
        
        
        postion_duplicate=[]
        position_regular=[]
        column="id"
        def get_data(column):
            time_1=[]
            
            
            
            for number in position:
                #print(int(number))
                creation_time = con.execute("""
                SELECT FileCreationTime,{} FROM {} WHERE file_number LIKE ?;""".format(column,row[0]), (int(number),)).fetchall()
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
                            postion_duplicate.append(number)
                        Z=[x for _, x in sorted(zip(time, number))]
                        #print(Z)
                        
                        time_1.append(Z)
                    else:
                        pass
            return time_1
        time_1_list=get_data(column)


        with con:
            con.execute("""
            ALTER TABLE {}
                ADD duplicate INTEGER;""".format(row[0]))
        
        with con:
            for n in time_1_list:
                for i in range(len(n)):
                    con.execute("""
                    UPDATE {}
                        SET duplicate = {}
                        WHERE id LIKE ?;""".format(row[0],i+1), (n[i],))
            

