from itertools import count
from logging import raiseExceptions
from multiprocessing.connection import wait
from operator import index
from textwrap import indent
from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from tqdm import *
import os
from zlib import adler32
import time
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen


name='Lund_GRID_all.db'
con = sl.connect('C:\\Users\\MSI PC\\Desktop\\gitproj\\LDCS-dark-data-qualitative-analysis\\Lund_GRID_all.db')


not_rucio=[]
rucio=[]
batches=[]

for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else: 
        x_values=[]
        y_values=[]
        print(row[0])
        duplicates=con.execute('SELECT ComputingElement FROM {} where duplicate is not Null'.format(row[0])).fetchall()
        #print(len(duplicates))
        duplicates = [x[0] for x in duplicates]
        duplicates2=list(set(duplicates))
        for n in duplicates2:
            y_values.append(duplicates.count(n))
            if n==None:
                n="None"
            x_values.append(n)
        x_values.append("Sum of all duplicates in Rucio")
        if "None" in x_values:
            n=x_values.index("None")
            y_values.append(sum(y_values)-y_values[n])
        import pandas as pd

        name=name.replace(".db","")
        name2=name.replace("_all","")
        name2=name.replace("_2","")
        name2=name2.replace("_"," ")
        """
        plt.bar(x_values,y_values)
        plt.xticks(rotation='horizontal')
        plt.title("{}: Procentage that is duplicate at different computing element".format(name2))

        plt.xlabel("Computing center")
        plt.grid()
        plt.ylabel("Procentage of files")
        #plt.show()
        manager = plt.get_current_fig_manager()
        manager.window.showMaximized()
        #plt.show()
        figure = plt.gcf()
        figure.set_size_inches(19, 10)"""
        if "None" in x_values:
            rucio.append(y_values[x_values.index("Sum of all duplicates in Rucio")])
            not_rucio.append(y_values[x_values.index("None")])
            batches.append(row[0])
print("\n\n"+name)
print("Dataset & In Rucio & Not in Rucio & Procentage not in Rucio \\\\")
for n in range(len(rucio)):

    print(batches[n]+" & " +str(rucio[n])+"  & "+str(not_rucio[n])+" & "+str(round(rucio[n]/(not_rucio[n])*100,3))+"%")

print(sum(rucio))
print(sum(not_rucio))