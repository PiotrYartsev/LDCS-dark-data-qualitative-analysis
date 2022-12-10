import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
import random 

from subprocess import PIPE, Popen
LUND_GRID='Lund_GRIDFTP_all_fixed_delete_all.db'
LUND_GRID_con = sl.connect(LUND_GRID)


same=[]
diffrent=[]
missing_dataset=[]


rows={}
for row in (LUND_GRID_con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        get_the_location_scope_and_filename = LUND_GRID_con.execute("SELECT DataLocation,scope, file FROM {} where duplicate > 0;".format(row[0])).fetchall()
        has_a_location = []
        does_not_have_a_location = []
        for location, scope, filename in get_the_location_scope_and_filename:
            if location == None:
                does_not_have_a_location.append((location, scope, filename))
            else:
                has_a_location.append((location, scope, filename))
        
        if len(has_a_location) > 2 and len(does_not_have_a_location) > 2:
            rows[row[0]]=[has_a_location,does_not_have_a_location]

file=open('mode1_mode2.txt','w')        
for key in rows:

    print(key)

    #print(rows[key][0])
    string_to_write_one=str(rows[key][0][0][1])+":"+str(rows[key][0][0][2])
    file.write(string_to_write_one)
    file.write("\n")
    
    for i in rows[key][0][1:]:
        string_to_write=str(i[2])
        #write the string to the text file as a new line
        file.write(string_to_write)
        file.write("\n")

    for i in rows[key][1]:
        string_to_write=str(i[2])
        #write the string to the text file as a new line
        file.write(string_to_write)
        file.write("\n")
        
    file.write(",")

            #write to a text file
            
file.close()
                   

