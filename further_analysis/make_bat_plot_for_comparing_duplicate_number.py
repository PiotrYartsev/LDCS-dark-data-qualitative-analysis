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


name2='Lund_all_fixed_delete_all.db'
con = sl.connect(name2)

"""
procentage_of_files=[]
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #sheck if there are duplicates
        

        number_of_duplciates=con.execute("Select count(*) from {} where duplicate is not Null".format(row[0])).fetchall()[0][0]

        if number_of_duplciates==0:
            pass
        else:
            number_of_files=con.execute("Select count(*) from {}".format(row[0])).fetchall()[0][0]
            procentage_of_files.append(round(100*number_of_duplciates/number_of_files,3))
#print(procentage_of_files)
# make a histogram
n, bins, patches = plt.hist(x=procentage_of_files, bins=100, color="tab:blue", rwidth=0.85)


#get the highest bin
highest_bin=max(n)
print(highest_bin)
#fit a decay curve to the data
#def decay_curve(x):
#largest bin*(fitting factor**x)
    


plt.xlabel('Distribution of duplicates in %')

#instead of xticks we want to have the number of files

name2=name.replace('.db','')
plt.xlim(0)
plt.ylabel('Frequency')
plt.title('Procentage of duplicates in files')
maxfreq = n.max()
#make the x axis a show % instead of numbers

# Set a clean upper y-axis limit.
plt.ylim(ymax=np.ceil(maxfreq / 10) * 10 if maxfreq % 10 else maxfreq + 10)



plt.savefig('figures/{}/bar-plot/procentage_of_duplicates.png'.format(name2))
plt.close()
"""
duplicate_length=[]
duplicate_length_2={}
for row in tqdm(con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #sheck if there are duplicates
        filenumbers2=con.execute("Select file_number from {} where duplicate > 0".format(row[0])).fetchall()

        number_of_duplciates2=len(filenumbers2)
        if number_of_duplciates2==0:
            pass
        else:
            for filenumber2 in filenumbers2:
                #get largest duplicate
                largest_duplicate=con.execute("Select max(duplicate) from {} where file_number={}".format(row[0],filenumber2[0])).fetchall()[0][0]
                if largest_duplicate==0 or largest_duplicate==1:
                    pass
                else:
                    if largest_duplicate in duplicate_length_2:
                        duplicate_length_2[largest_duplicate]+=1
                    else:
                        duplicate_length_2[largest_duplicate]=1



print(duplicate_length_2)
plt.bar(duplicate_length_2.keys(),duplicate_length_2.values(),color='tab:blue')
#make the x-ticks show the number of duplicates
#make x-ticks be in steps of 2
plt.xticks(np.arange(0, max(duplicate_length_2.keys())+1, 2.0))



#start the x-axis at 2 minus the width of the first bar
#plt.xlim(0)




plt.xlabel('Duplicate chain length')
plt.ylabel('Frequency')
plt.title('Duplicate chain length in files')
plt.show()
#plt.savefig('figures/{}/bar-plot/duplicate_length.png'.format(name2)#)
#"""