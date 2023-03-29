from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from tqdm import *


#function that does all the same stuff
def run_for_table(table):
    con=sl.connect(table)

    #define the variables for duplicates and non_duplicates
    minimum_File_Creation_Time_duplicate=0
    maximum_File_Creation_Time_duplicate=0

    minimum_File_Creation_Time=0
    maximum_File_Creation_Time=0

    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else:
            #Get the largest and smallest FileCreationTime for files with duplicates
            get_minimum_File_Creation_Time_duplicate = con.execute('SELECT MIN(FileCreationTime) FROM {} where duplicate > 0'.format(row[0])).fetchone()[0]
            get_maximum_File_Creation_Time_duplicate = con.execute('SELECT MAX(FileCreationTime) FROM {} where duplicate > 0'.format(row[0])).fetchone()[0]
            #if it exists
            if get_minimum_File_Creation_Time_duplicate is not None:
                #if it is the first time
                if minimum_File_Creation_Time_duplicate == 0:
                    minimum_File_Creation_Time_duplicate = get_minimum_File_Creation_Time_duplicate
                
                else:
                    #sheck if it is smaller than the current minimum
                    if get_minimum_File_Creation_Time_duplicate<minimum_File_Creation_Time_duplicate and minimum_File_Creation_Time_duplicate>0:
                        minimum_File_Creation_Time_duplicate=get_minimum_File_Creation_Time_duplicate
                #check if it is larger than the current maximum
                if get_maximum_File_Creation_Time_duplicate>maximum_File_Creation_Time_duplicate:
                    maximum_File_Creation_Time_duplicate=get_maximum_File_Creation_Time_duplicate


            #files without duplicates
            get_minimum_File_Creation_Time=con.execute('SELECT MIN(FileCreationTime) FROM {}'.format(row[0])).fetchone()[0]
            get_maximum_File_Creation_Time=con.execute('SELECT MAX(FileCreationTime) FROM {}'.format(row[0])).fetchone()[0]


            #same as above
            if minimum_File_Creation_Time == 0:
                minimum_File_Creation_Time = get_minimum_File_Creation_Time
            else:
                if get_minimum_File_Creation_Time<minimum_File_Creation_Time and minimum_File_Creation_Time>0:
                    minimum_File_Creation_Time=get_minimum_File_Creation_Time

            if get_maximum_File_Creation_Time>maximum_File_Creation_Time:
                maximum_File_Creation_Time=get_maximum_File_Creation_Time
    return minimum_File_Creation_Time, maximum_File_Creation_Time, minimum_File_Creation_Time_duplicate, maximum_File_Creation_Time_duplicate

#run the fucntion for the different storage locations and print out the result

table='Lund_all_fixed_delete_all.db'
minimum_File_Creation_TimeLUND, maximum_File_Creation_TimeLUND, minimum_File_Creation_Time_duplicateLUND, maximum_File_Creation_Time_duplicateLUND = run_for_table(table)

print("\n")
print(table)
minimum_File_Creation_TimeLUND = datetime.fromtimestamp(minimum_File_Creation_TimeLUND)
maximum_File_Creation_TimeLUND = datetime.fromtimestamp(maximum_File_Creation_TimeLUND)
print("The timeframe for regular files:" + str(minimum_File_Creation_TimeLUND) +" - "+ str(maximum_File_Creation_TimeLUND))

minimum_File_Creation_Time_duplicateLUND = datetime.fromtimestamp(minimum_File_Creation_Time_duplicateLUND)
maximum_File_Creation_Time_duplicateLUND = datetime.fromtimestamp(maximum_File_Creation_Time_duplicateLUND)
print("The timeframe for duplicate files:" + str(minimum_File_Creation_Time_duplicateLUND) +" - "+ str(maximum_File_Creation_Time_duplicateLUND))





table='Lund_GRIDFTP_all_fixed_delete_all.db'

minimum_File_Creation_TimeLUNDGRID, maximum_File_Creation_TimeLUNDGRID, minimum_File_Creation_Time_duplicateLUNDGRID, maximum_File_Creation_Time_duplicateLUNDGRID = run_for_table(table)

print("\n")
print(table)
minimum_File_Creation_TimeLUNDGRID = datetime.fromtimestamp(minimum_File_Creation_TimeLUNDGRID)
maximum_File_Creation_TimeLUNDGRID = datetime.fromtimestamp(maximum_File_Creation_TimeLUNDGRID)
print("The timeframe for regular files:" + str(minimum_File_Creation_TimeLUNDGRID) +" - "+ str(maximum_File_Creation_TimeLUNDGRID))


minimum_File_Creation_Time_duplicateLUNDGRID = datetime.fromtimestamp(minimum_File_Creation_Time_duplicateLUNDGRID)
maximum_File_Creation_Time_duplicateLUNDGRID = datetime.fromtimestamp(maximum_File_Creation_Time_duplicateLUNDGRID)
print("The timeframe for duplicate files:" + str(minimum_File_Creation_Time_duplicateLUNDGRID) +" - "+ str(maximum_File_Creation_Time_duplicateLUNDGRID))

table='SLAC_mc20_delete_all.db'

minimum_File_Creation_TimeSLAC, maximum_File_Creation_TimeSLAC, minimum_File_Creation_Time_duplicateSLAC, maximum_File_Creation_Time_duplicateSLAC = run_for_table(table)

print("\n")
print(table)
minimum_File_Creation_TimeSLAC = datetime.fromtimestamp(minimum_File_Creation_TimeSLAC)
maximum_File_Creation_TimeSLAC = datetime.fromtimestamp(maximum_File_Creation_TimeSLAC)
print("The timeframe for regular files:" + str(minimum_File_Creation_TimeSLAC) +" - "+ str(maximum_File_Creation_TimeSLAC))


minimum_File_Creation_Time_duplicateSLAC = datetime.fromtimestamp(minimum_File_Creation_Time_duplicateSLAC)
maximum_File_Creation_Time_duplicateSLAC = datetime.fromtimestamp(maximum_File_Creation_Time_duplicateSLAC)
print("The timeframe for duplicate files:" + str(minimum_File_Creation_Time_duplicateSLAC) +" - "+ str(maximum_File_Creation_Time_duplicateSLAC))




