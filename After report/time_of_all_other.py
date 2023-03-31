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

#rowList=['v171targetPNrecotskimsubrun1batch1', 'v171targetPNrecotskimsubrun1batch2', 'v171targetPNrecotskimsubrun1batch3', 'v171targetPNrecotskimsubrun1batch4', 'v171targetPNrecotskimsubrun1batch5', 'v171targetPNrecotskimsubrun2batch1', 'v171targetPNrecotskimsubrun2batch2', 'v171targetPNrecotskimsubrun2batch3', 'v171targetPNrecotskimsubrun2batch4', 'v171targetPNrecotskimsubrun2batch5', 'v171targetPNrecotskimsubrun3batch1', 'v171targetPNrecotskimsubrun3batch2', 'v171targetPNrecotskimsubrun3batch3', 'v171targetPNrecotskimsubrun3batch4', 'v171targetPNrecotskimsubrun3batch5', 'v171targetPNrecotskimsubrun4batch3', 'v171targetPNrecotskimsubrun4batch4', 'v171targetPNrecotskimsubrun4batch5', 'v300overlayEcalPNbatch1', 'v300overlayEcalPNbatch2', 'v300overlayEcalPNbatch3', 'v300overlayEcalPNbatch4']


time_dup=[]
time_regural1=[]
for rows in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    row=rows[0]
    if row == 'sqlite_sequence':
        pass
    else:
        print(row)
        #for each file that is a duplciate, retrive the filenumber,id and the computing location 
        times_of_duplicates=con.execute('SELECT FileCreationTime FROM '+row+' WHERE duplicate>0').fetchall()
        #print(len(times_of_duplicates))
        times_of_duplicates=[x[0] for x in times_of_duplicates]
        time_dup.extend(times_of_duplicates)

        time_regural=con.execute('SELECT FileCreationTime FROM '+row).fetchall()
        #print(len(time_regural))
        time_regural=[x[0] for x in time_regural]
        time_regural1.extend(time_regural)

print(len(time_dup))
print(len(time_regural1))

#save duplicates and regural in a file
dup=open('time_dup.txt','w')
for i in time_dup:
    dup.write(str(i)+'\n')
dup.close()

#save duplicates and regural in a file
reg=open('time_reg.txt','w')
for i in time_regural1:
    reg.write(str(i)+'\n')
reg.close()

