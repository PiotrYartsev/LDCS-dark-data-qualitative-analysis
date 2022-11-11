from asyncio import run_coroutine_threadsafe
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
con = sl.connect('/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/{}'.format(name))
#con=sl.connect("C:\\\\Users\\\\MSI PC\\\\Desktop\\\\gitproj\\\\LDCS-dark-data-qualitative-analysis\\\\{}".format(name))
def run_for_all():
    not_rucio=[]
    rucio=[]
    batches=[]


    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else: 
            print(row[0])

            x_values=[]
            y_values=[]
            duplicates=con.execute("SELECT ComputingElement FROM {} where duplicate is not Null".format(row[0])).fetchall()
            print(len(duplicates))
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

            
            if "None" in x_values:
                rucio.append(y_values[x_values.index("Sum of all duplicates in Rucio")])
                not_rucio.append(y_values[x_values.index("None")])
                batches.append(row[0])
    return rucio,not_rucio,batches

rucio,not_rucio,batches=run_for_all()
data={}
#data['index']=["100%","90-100%"]
for n in range(len(rucio)):
    if rucio[n]/(not_rucio[n])==1:
        if "100%" in data:
            data["100%"]=data["100%"]+1
        else:
            data["100%"]=1
    elif rucio[n]/(not_rucio[n])>0.9:
        if "90-100%" in data:
            data["90-100%"]=data["90-100%"]+1
        else:
            data["90-100%"]=1
    else:
        if "Not matching" in data:
            data["Not matching"]=data["Not matching"]+1
        else:
            data["Not matching"]=1

def plotandstuff(ruci,not_ruci,batch,name):
    name=name.replace(".db","")
    name2=name.replace("_all","")
    name2=name.replace("_2","")
    name2=name2.replace("_"," ")


    total_number_of_duplicates=sum(rucio)

    print(sum(rucio))
    print(sum(not_rucio))
    courses = list(data.keys())
    values = list(data.values())
    
    fig = plt.figure(figsize = (10, 5))
    
    # creating the bar plot
    plt.bar(courses, values, color ='tab:blue',
            width = 0.4)
    
    plt.xlabel("Porcentage of duplicates in dataset %")
    plt.title("{}: How many datasets have equal number of duplicates in Rucio and not in Rucio".format(name2))
    #plt.show()


def run_for_Is_Reco_notNull():
    not_rucio=[]
    rucio=[]
    batches=[]


    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else: 
            print(row[0])

            x_values=[]
            y_values=[]
            duplicates=con.execute("SELECT ComputingElement,IsRecon FROM {} where duplicate is not Null".format(row[0])).fetchall()
            #print(len(duplicates))
            duplicates = [x[0] for x in duplicates]
            recon=[x[1] for x in duplicates]
            duplicates2=list(set(duplicates))
            print(recon)
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

            
            if "None" in x_values:
                rucio.append(y_values[x_values.index("Sum of all duplicates in Rucio")])
                not_rucio.append(y_values[x_values.index("None")])
                batches.append(row[0])
    return rucio,not_rucio,batches

rucio,not_rucio,batches=run_for_Is_Reco_notNull()
plotandstuff(rucio,not_rucio,batches,name)

rucio,not_rucio,batches=run_for_all()
plotandstuff(rucio,not_rucio,batches,name)