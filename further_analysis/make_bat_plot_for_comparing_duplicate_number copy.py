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


name='SLAC_mc20_delete_all.db'
con = sl.connect(name)


reconstructed_files_dup=0
reconstructed_files=0

regular_files_dup=0
regular_files=0
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #print(row[0])
        #check if there are reconstructed files

        are_there_reconstructed_files=con.execute("Select count(*) from {} where IsRecon is 'True'".format(row[0])).fetchall()[0][0]
        if are_there_reconstructed_files==0:
            pass
        else:
            reconstructed_file=con.execute("Select duplicate from {}".format(row[0])).fetchall()

            reconstructed_file=[x[0] for x in reconstructed_file]
            for n in reconstructed_file:
                if n is None:
                    reconstructed_files+=1
                else:
                    reconstructed_files_dup+=1


for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #print(row[0])
        #check if there are reconstructed files

        are_there_reconstructed_files=con.execute("Select count(*) from {} where IsRecon is 'True'".format(row[0])).fetchall()[0][0]
        if are_there_reconstructed_files==0:
            regular_file=con.execute("Select duplicate from {}".format(row[0])).fetchall()

            regular_file=[x[0] for x in regular_file]
            for n in regular_file:
                if n is None:
                    regular_files+=1
                else:
                    regular_files_dup+=1
print(name)
print("Procentage of duplicates in reconstructed files",round(100*reconstructed_files_dup/(reconstructed_files+reconstructed_files_dup),1),"%")
print("Procentage of duplicates in regular files",round(100*regular_files_dup/(regular_files+regular_files_dup),1),"%")