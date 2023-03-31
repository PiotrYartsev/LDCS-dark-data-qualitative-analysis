from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from tqdm import *

from sqlalchemy import column
name='Lund_GRIDFTP_all_fixed_delete_all.db'
con = sl.connect(name)

rowList=['v171targetPNrecotskimsubrun1batch1', 'v171targetPNrecotskimsubrun1batch2', 'v171targetPNrecotskimsubrun1batch3', 'v171targetPNrecotskimsubrun1batch4', 'v171targetPNrecotskimsubrun1batch5', 'v171targetPNrecotskimsubrun2batch1', 'v171targetPNrecotskimsubrun2batch2', 'v171targetPNrecotskimsubrun2batch3', 'v171targetPNrecotskimsubrun2batch4', 'v171targetPNrecotskimsubrun2batch5', 'v171targetPNrecotskimsubrun3batch1', 'v171targetPNrecotskimsubrun3batch2', 'v171targetPNrecotskimsubrun3batch3', 'v171targetPNrecotskimsubrun3batch4', 'v171targetPNrecotskimsubrun3batch5', 'v171targetPNrecotskimsubrun4batch3', 'v171targetPNrecotskimsubrun4batch4', 'v171targetPNrecotskimsubrun4batch5', 'v300overlayEcalPNbatch1', 'v300overlayEcalPNbatch2', 'v300overlayEcalPNbatch3', 'v300overlayEcalPNbatch4']


time=[]
for row in rowList:
    print(row)
    #for each file that is a duplciate, retrive the filenumber,id and the computing location 
    list_of_duplicates=con.execute('SELECT file_number, id, FileCreationTime FROM '+row+' WHERE duplicate>0 and ComputingElement is not NULL').fetchall()
    ids=[a[1] for a in list_of_duplicates]
    filenumbers=[a[0] for a in list_of_duplicates]
    times=[a[2] for a in list_of_duplicates]
    set_of_filenumbers=set(filenumbers)
    #count the number of times each file number is duplicated
    files_to_check_further={}
    
    for file_number in set_of_filenumbers:
        
        if filenumbers.count(file_number)>1:
            #find postion of all ocourances of this filenumber
            positions=[i for i, x in enumerate(filenumbers) if x == file_number]
            #find the id of all ocourances of this filenumber
            ids_of_duplicates=[ids[i] for i in positions]
            temp=[times[i] for i in positions]
            #add on to the list of times
            time.extend(temp)
            #find the computing location of all ocourances of this filenumber
            files_to_check_further[file_number]=ids_of_duplicates

    """
    #print(files_to_check_further)
    #print(len(list_of_duplicates))
    p=0
    for key in files_to_check_further:
        p+=len(files_to_check_further[key])
    #print(p)
    print(p/len(list_of_duplicates)*100)
    """
print(time)
#save to txt file at C:\Users\piotr\Documents\GitHub\LDCS-dark-data-qualitative-analysis

save_file=open('C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\time_of_duplciates_in_rucio.txt','w')

for t in time:
    save_file.write(str(t)+'\n')
save_file.close()

#"""
