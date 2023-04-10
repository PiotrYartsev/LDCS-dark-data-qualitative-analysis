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

file_to_write=open('list_of_duplicates.txt','w')
for row in tqdm(rowList):
    print(row)
    filenumbers=con.execute('SELECT file_number FROM '+row+' WHERE duplicate>0 and ComputingElement is not NULL').fetchall()
    filenumbers=[a[0] for a in filenumbers]
    #print(len(filenumbers))
    repeated_filenumbers=[item for item in filenumbers if filenumbers.count(item) > 1]
    for filenumber in tqdm(repeated_filenumbers):
        scope_and_file=con.execute('SELECT scope, file FROM '+row+' WHERE file_number='+str(filenumber)+' and ComputingElement is not NULL').fetchall()
        scope=[a[0] for a in scope_and_file]
        file=[a[1] for a in scope_and_file]
        for i in range(len(scope)):
            file_to_write.write(scope[i]+':'+file[i]+',')
        file_to_write.write('\n')
file_to_write.close()

