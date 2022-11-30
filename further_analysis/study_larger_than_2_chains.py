import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 

from subprocess import PIPE, Popen
delete_all_name='Lund_GRIDFTP_all_fixed_delete_all.db'
delete_all = sl.connect('{}'.format(delete_all_name))




tables=[]
length=[]
for row in (delete_all.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #get max duplicate number
        max_duplicate=delete_all.execute("SELECT max(duplicate) from {};".format(row[0])).fetchall()[0][0]
        if max_duplicate is not None:
            if max_duplicate>2:
                tables.append(row[0])
                length.append(max_duplicate)
for table in tables:
    print(table, length[tables.index(table)])


stuff={}

"""
for row in (delete_all.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        file_number_dup_more_then2=delete_all.execute("SELECT file_number from {} where duplicate>2;".format(row[0])).fetchall()


        if len(file_number_dup_more_then2)>0:
            file_number_dup_more_then2=[i[0] for i in file_number_dup_more_then2]

            file_number_dup_more_then2=list(set(file_number_dup_more_then2))

            #get the DataLocations for each file_number
            for file_number in file_number_dup_more_then2:
                data_location=delete_all.execute("SELECT DataLocation from {} where file_number={};".format(row[0], file_number)).fetchall()
                data_location=[i[0] for i in data_location]
                data_location2=list(set(data_location))
                for i in data_location2:
                    if i is not None:
                        if data_location.count(i)>1:
                            print(data_location.count(i), row[0], file_number, i)
                            """