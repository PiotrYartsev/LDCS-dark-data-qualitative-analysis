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
name_Lund_GRID='Lund_GRIDFTP_all_fixed_delete_all.db'
Lund_GRID = sl.connect(name_Lund_GRID)

name_Lund='Lund_all_fixed_delete_all.db'
Lund = sl.connect(name_Lund)

name_SLAC='SLAC_mc20_delete_all.db'

SLAC = sl.connect(name_SLAC)

number_of_files_LUND_GRIDFTP_all_fixed_delete_all=0
number_of_files_LUND_all_fixed_delete_all=0
number_of_files_SLAC_mc20_delete_all=0


number_of_duplicates_LUND_GRIDFTP_all_fixed_delete_all=0
number_of_duplicates_LUND_all_fixed_delete_all=0
number_of_duplicates_SLAC_mc20_delete_all=0

number_of_duplicates_minus_one_LUND_GRIDFTP_all_fixed_delete_all=0
number_of_duplicates_minus_one_LUND_all_fixed_delete_all=0
number_of_duplicates_minus_one_SLAC_mc20_delete_all=0



longest_chain_LUND_GRIDFTP_all_fixed_delete_all=0
longest_chain_LUND_all_fixed_delete_all=0
longest_chain_SLAC_mc20_delete_all=0

for row in tqdm(Lund_GRID.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        number_of_files=Lund_GRID.execute('SELECT COUNT (*) FROM '+row[0]).fetchall()
        number_of_files=number_of_files[0][0]
        
        number_of_files_LUND_GRIDFTP_all_fixed_delete_all+=number_of_files


        number_of_duplicates=Lund_GRID.execute('SELECT COUNT (*) FROM '+row[0]+' WHERE duplicate>0').fetchall()
        if number_of_duplicates[0][0]>0:
            number_of_duplicates=number_of_duplicates[0][0]
            number_of_duplicates_LUND_GRIDFTP_all_fixed_delete_all+=number_of_duplicates

        number_of_duplicates_one=Lund_GRID.execute('SELECT COUNT (*) FROM '+row[0]+' WHERE duplicate=1').fetchall()
        if number_of_duplicates_one[0][0]>0:
            number_of_duplicates_one=number_of_duplicates_one[0][0]
            number_of_duplicates_minus_one_LUND_GRIDFTP_all_fixed_delete_all+=number_of_duplicates_one

        longest_chain=Lund_GRID.execute('SELECT MAX (duplicate) FROM '+row[0]).fetchall()
        longest_chain=longest_chain[0][0]
        try:
            if longest_chain>longest_chain_LUND_GRIDFTP_all_fixed_delete_all:
                longest_chain_LUND_GRIDFTP_all_fixed_delete_all=longest_chain
        except:
            pass

for row in tqdm(Lund.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        number_of_files=Lund.execute('SELECT COUNT (*) FROM '+row[0]).fetchall()
        number_of_files=number_of_files[0][0]
        number_of_files_LUND_all_fixed_delete_all+=number_of_files

        number_of_duplicates=Lund.execute('SELECT COUNT (*) FROM '+row[0]+' WHERE duplicate>0').fetchall()

        if number_of_duplicates[0][0]>0:
            number_of_duplicates=number_of_duplicates[0][0]
            number_of_duplicates_LUND_all_fixed_delete_all+=number_of_duplicates
                
        number_of_duplicates_one=Lund.execute('SELECT COUNT (*) FROM '+row[0]+' WHERE duplicate=1').fetchall()
        if number_of_duplicates_one[0][0]>0:
            number_of_duplicates_one=number_of_duplicates_one[0][0]
            number_of_duplicates_minus_one_LUND_all_fixed_delete_all+=number_of_duplicates_one


        longest_chain=Lund.execute('SELECT MAX (duplicate) FROM '+row[0]).fetchall()
        longest_chain=longest_chain[0][0]
        try:
            if longest_chain>longest_chain_LUND_all_fixed_delete_all:
                longest_chain_LUND_all_fixed_delete_all=longest_chain
        except:
            pass

for row in tqdm(SLAC.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        number_of_files=SLAC.execute('SELECT COUNT (*) FROM '+row[0]).fetchall()
        number_of_files=number_of_files[0][0]
        number_of_files_SLAC_mc20_delete_all+=number_of_files

        number_of_duplicates=SLAC.execute('SELECT COUNT (*) FROM '+row[0]+' WHERE duplicate>0').fetchall()
        if number_of_duplicates[0][0]>0:
            number_of_duplicates=number_of_duplicates[0][0]
            number_of_duplicates_SLAC_mc20_delete_all+=number_of_duplicates


        number_of_duplicates_one=SLAC.execute('SELECT COUNT (*) FROM '+row[0]+' WHERE duplicate=1').fetchall()
        if number_of_duplicates_one[0][0]>0:
            number_of_duplicates_one=number_of_duplicates_one[0][0]
            number_of_duplicates_minus_one_SLAC_mc20_delete_all+=number_of_duplicates_one


        longest_chain=SLAC.execute('SELECT MAX (duplicate) FROM '+row[0]).fetchall()
        longest_chain=longest_chain[0][0]
        try:
            if longest_chain>longest_chain_SLAC_mc20_delete_all:
                longest_chain_SLAC_mc20_delete_all=longest_chain
        except:
            pass


print("Title    Lund    Lund GRIDFTP    SLAC")
print("Number of files"+"  &  "+str(number_of_files_LUND_all_fixed_delete_all)+"  &  "+str(number_of_files_LUND_GRIDFTP_all_fixed_delete_all)+" & "+str(number_of_files_SLAC_mc20_delete_all)+"\\\\ \\hline")

print("Number of duplicates"+" & "+str(number_of_duplicates_LUND_all_fixed_delete_all)+" & "+str(number_of_duplicates_LUND_GRIDFTP_all_fixed_delete_all)+" & "+str(number_of_duplicates_SLAC_mc20_delete_all)+"\\\\ \\hline")

print("Procent of duplicates"+" & "+str(100*round(number_of_duplicates_LUND_all_fixed_delete_all/(number_of_files_LUND_all_fixed_delete_all),3))+"\\%"+" & "+str(100*round(number_of_duplicates_LUND_GRIDFTP_all_fixed_delete_all/(number_of_files_LUND_GRIDFTP_all_fixed_delete_all),3))+"\\%"+" & "+str(100*round(number_of_duplicates_SLAC_mc20_delete_all/(number_of_files_SLAC_mc20_delete_all),3))+"\\%" +"\\\\ \\hline")

print("Number of duplicate\\\\after removing the first "+" & "+str(number_of_duplicates_LUND_all_fixed_delete_all-number_of_duplicates_minus_one_LUND_all_fixed_delete_all)+" & "+str(number_of_duplicates_LUND_GRIDFTP_all_fixed_delete_all-number_of_duplicates_minus_one_LUND_GRIDFTP_all_fixed_delete_all)+" & "+str(number_of_duplicates_SLAC_mc20_delete_all-number_of_duplicates_minus_one_SLAC_mc20_delete_all)+"\\\\ \\hline")

print("Procent of duplicate\\\\after removing the first "+" & "+str(100*round((number_of_duplicates_LUND_all_fixed_delete_all-number_of_duplicates_minus_one_LUND_all_fixed_delete_all)/(number_of_files_LUND_all_fixed_delete_all),3))+"\\%"+" & "+str(100*round((number_of_duplicates_LUND_GRIDFTP_all_fixed_delete_all-number_of_duplicates_minus_one_LUND_GRIDFTP_all_fixed_delete_all)/(number_of_files_LUND_GRIDFTP_all_fixed_delete_all),3))+"\\%"+" & "+str(100*round((number_of_duplicates_SLAC_mc20_delete_all-number_of_duplicates_minus_one_SLAC_mc20_delete_all)/(number_of_files_SLAC_mc20_delete_all),3))+"\\%" +"\\\\ \\hline")



print("Longest chain"+" & "+str(longest_chain_LUND_all_fixed_delete_all)+" & "+str(longest_chain_LUND_GRIDFTP_all_fixed_delete_all)+" & "+str(longest_chain_SLAC_mc20_delete_all)+"\\\\ \\hline")