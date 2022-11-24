from logging import raiseExceptions
from tokenize import Number

from tqdm import *

from subprocess import PIPE, Popen

from zlib import adler32
import sqlite3 as sl

from subprocess import PIPE, Popen




def delte_copyies(dataset):
    con = sl.connect(dataset, check_same_thread=False)

    #Retrive all tables from the database
    all_batches=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    all_batches=[a[0] for a in all_batches]
    for row in all_batches:
        #If thea table is the table of tables, skip it
        if row == 'sqlite_sequence':
            pass
        else:

            print("          "+row+"\n")
            all_data={}
            retrive_file_location_and_batch_id=con.execute("Select id,DataLocation,file,Computingelement,BatchID from {};".format(row)).fetchall()
            for thing in retrive_file_location_and_batch_id:
                id=thing[0]
                text=str(thing[1])+str(thing[2])+str(thing[3])+str(thing[4])
                if text in all_data:
                    all_data[text].append(id)
                else:
                    all_data[text]=[id]
            number=0
            for key in all_data:
                number=number+len(all_data[key])-1
            print("Removing {} duplicates".format(number))
            #print the length of the table
            number_of_rows=con.execute("Select count(*) from {};".format(row)).fetchall()[0][0]
            print(number_of_rows)
            for key in tqdm(all_data):
                #remove all but the first one
                
                if len(all_data[key])>1:
                    
                    for i in range(1,len(all_data[key])):
                        con.execute("Delete from {} where id={};".format(row,all_data[key][i]))
                        con.commit()
            #print the length of the table
            number_of_rows=con.execute("Select count(*) from {};".format(row)).fetchall()[0][0]
            print(number_of_rows)
delte_copyies('Lund_all_fixed_copy1.db')
