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



con = sl.connect('duplicate_data_2.db')
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
            time_1_list=[]
            time_2_list=[]
            
            
            for number in position:
                #print(int(number))
                creation_time = con.execute("""
                SELECT FileCreationTime,{} FROM {} WHERE file_number LIKE ?;""".format(column,row[0]), (int(number),)).fetchall()
                #print(creation_time)
                if len(creation_time)==0:
                    pass
                else:
                    if len(creation_time)>1:
                       # print(creation_time)
                        time1=creation_time[0][0]
                        time2=creation_time[1][0]
                        number1=(creation_time[0][1])
                        number2=(creation_time[1][1])
                        postion_duplicate.append(number)
                        if time1>time2:
                            time_1_list.append(number2)
                            time_2_list.append(number1)
                        else:
                            time_1_list.append(number1)
                            time_2_list.append(number2)
                    else:
                        pass
            return time_1_list,time_2_list
        time_1_list,time_2_list=get_data(column)

        with con:
            con.execute("""
            DELETE FROM {}
                WHERE file IS NULL;""".format(row[0]))

        with con:
            con.execute("""
            ALTER TABLE {}
                ADD duplicate INTEGER;""".format(row[0]))
        
        with con:
            for number in time_1_list:
                con.execute("""
                UPDATE {}
                    SET duplicate = 1
                    WHERE id LIKE ?;""".format(row[0]), (number,))
            for number in time_2_list:
                con.execute("""
                UPDATE {}
                    SET duplicate = 2
                    WHERE id LIKE ?;""".format(row[0]), (number,))

