from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl

from sqlalchemy import column
from sympy import linsolve





def get_data(name):
    con = sl.connect('{}'.format(name))
    reconstructed_files=[]
    reconstructed_duplicate_files=[]

    files=[]
    duplicate_files=[]
    
    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else:
            #print(row[0])
            check_if_reconstructions=con.execute("SELECT COUNT (*) FROM "+row[0]+" WHERE IsRecon is 'True'").fetchone()
            check_if_reconstructions=check_if_reconstructions[0]
            if check_if_reconstructions > 0:
                #there are reconstructions
                number_of_reconstructed_files_in_table=con.execute("SELECT COUNT (*) FROM "+row[0]+" WHERE IsRecon is 'True'").fetchone()

                number_of_reconstructed_files_in_table=number_of_reconstructed_files_in_table[0]

                number_of_reconstructed_duplicate_files_in_table=con.execute("SELECT COUNT (*) FROM "+row[0]+" WHERE IsRecon is 'True' AND duplicate>0").fetchone()
                number_of_reconstructed_duplicate_files_in_table=number_of_reconstructed_duplicate_files_in_table[0]

                reconstructed_files.append(number_of_reconstructed_files_in_table)
                reconstructed_duplicate_files.append(number_of_reconstructed_duplicate_files_in_table)
            
            #there are no reconstructions
            number_of_files_in_table=con.execute("SELECT COUNT (*) FROM "+row[0]).fetchone()
            number_of_files_in_table=number_of_files_in_table[0]

            number_of_duplicate_files_in_table=con.execute("SELECT COUNT (*) FROM "+row[0]+" WHERE duplicate>0").fetchone()
            number_of_duplicate_files_in_table=number_of_duplicate_files_in_table[0]

            files.append(number_of_files_in_table)
            duplicate_files.append(number_of_duplicate_files_in_table)
    return files, duplicate_files, reconstructed_files, reconstructed_duplicate_files




nameLund='Lund_all_fixed_delete_all.db'
filesLund, duplicate_filesLund, reconstructed_filesLund, reconstructed_duplicate_filesLund=get_data(nameLund)

print(nameLund)
print("The procentage of regular files that are duplciates are: ", round(sum(duplicate_filesLund)*100/sum(filesLund),3), "\\%")

print("The procentage of reconstructed files that are duplciates are: ", round(sum(reconstructed_duplicate_filesLund)*100/sum(reconstructed_filesLund),3), "\\%")

reconstructed_duplicate_files_non_zeroLund=[a for a in reconstructed_duplicate_filesLund if a != 0]

print("The procentage of reconstructed tables that have duplicates are: ", round(len(reconstructed_duplicate_files_non_zeroLund)*100/len(reconstructed_filesLund),3), "\\%")

duplicate_files_non_zeroLund=[a for a in duplicate_filesLund if a != 0]
print("The procentage of regular tables that have duplicates are: ", round(len(duplicate_files_non_zeroLund)*100/len(filesLund),3), "\\%")









nameLUNDGRID='Lund_GRIDFTP_all_fixed_delete_all.db'
filesLUNDGRID, duplicate_filesLUNDGRID, reconstructed_filesLUNDGRID, reconstructed_duplicate_filesLUNDGRID=get_data(nameLUNDGRID)

print(nameLUNDGRID)
print("The procentage of regular files that are duplciates are: ", round(sum(duplicate_filesLUNDGRID)*100/sum(filesLUNDGRID),3), "\\%")

print("The procentage of reconstructed files that are duplciates are: ", round(sum(reconstructed_duplicate_filesLUNDGRID)*100/sum(reconstructed_filesLUNDGRID),3), "\\%")

reconstructed_duplicate_files_non_zeroLUNDGRID=[a for a in reconstructed_duplicate_filesLUNDGRID if a != 0]

print("The procentage of reconstructed tables that have duplicates are: ", round(len(reconstructed_duplicate_files_non_zeroLUNDGRID)*100/len(reconstructed_filesLUNDGRID),3), "\\%")

duplicate_files_non_zeroLUNDGRID=[a for a in duplicate_filesLUNDGRID if a != 0]
print("The procentage of regular tables that have duplicates are: ", round(len(duplicate_files_non_zeroLUNDGRID)*100/len(filesLUNDGRID),3), "\\%")





nameSLAC='SLAC_mc20_delete_all.db'

filesSLAC, duplicate_filesSLAC, reconstructed_filesSLAC, reconstructed_duplicate_filesSLAC=get_data(nameSLAC)

print(nameSLAC)
print("The procentage of regular files that are duplciates are: ", round(sum(duplicate_filesSLAC)*100/sum(filesSLAC),3), "\\%")

print("The procentage of reconstructed files that are duplciates are: ", round(sum(reconstructed_duplicate_filesSLAC)*100/sum(reconstructed_filesSLAC),3), "\\%")

reconstructed_duplicate_files_non_zeroSLAC=[a for a in reconstructed_duplicate_filesSLAC if a != 0]

print("The procentage of reconstructed tables that have duplicates are: ", round(len(reconstructed_duplicate_files_non_zeroSLAC)*100/len(reconstructed_filesSLAC),3), "\\%")

duplicate_files_non_zeroSLAC=[a for a in duplicate_filesSLAC if a != 0]

print("The procentage of regular tables that have duplicates are: ", round(len(duplicate_files_non_zeroSLAC)*100/len(filesSLAC),3), "\\%")




print("& "+nameLund +" & " + nameLUNDGRID + " & " + nameSLAC + " \\\\") 
print("The procentage of regular files that are duplciates are: & ", round(sum(duplicate_filesLund)*100/sum(filesLund),3), "\\% & ", round(sum(duplicate_filesLUNDGRID)*100/sum(filesLUNDGRID),3), "\\% & ", round(sum(duplicate_filesSLAC)*100/sum(filesSLAC),3), "\\% \\\\")
print("The procentage of reconstructed files that are duplciates are: & ", round(sum(reconstructed_duplicate_filesLund)*100/sum(reconstructed_filesLund),3), "\\% & ", round(sum(reconstructed_duplicate_filesLUNDGRID)*100/sum(reconstructed_filesLUNDGRID),3), "\\% & ", round(sum(reconstructed_duplicate_filesSLAC)*100/sum(reconstructed_filesSLAC),3), "\\% \\\\")
print("The procentage of reconstructed tables that have duplicates are: & ", round(len(reconstructed_duplicate_files_non_zeroLund)*100/len(reconstructed_filesLund),3), "\\% & ", round(len(reconstructed_duplicate_files_non_zeroLUNDGRID)*100/len(reconstructed_filesLUNDGRID),3), "\\% & ", round(len(reconstructed_duplicate_files_non_zeroSLAC)*100/len(reconstructed_filesSLAC),3), "\\% \\\\")
print("The procentage of regular tables that have duplicates are: & ", round(len(duplicate_files_non_zeroLund)*100/len(filesLund),3), "\\% & ", round(len(duplicate_files_non_zeroLUNDGRID)*100/len(filesLUNDGRID),3), "\\% & ", round(len(duplicate_files_non_zeroSLAC)*100/len(filesSLAC),3), "\\% \\\\")