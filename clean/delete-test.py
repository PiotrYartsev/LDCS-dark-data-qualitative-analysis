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


name='Lund_all_copy.db'

#con=sl.connect("C:\\\\Users\\\\MSI PC\\\\Desktop\\\\gitproj\\\\LDCS-dark-data-qualitative-analysis\\\\{}".format(name))



def add_time(database):
    con = sl.connect(database)
    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else: 
            #print(row[0])
            scopes=con.execute('SELECT DISTINCT scope FROM {};'.format(row[0])).fetchall()
            scopes=[x[0] for x in scopes]
            scopes=[x.replace(" ","") for x in scopes]
            for scope in scopes:
                if scope == None:
                    pass
                else:   
                    if 'validation' in scope or 'test' in scope:
                        if len(scopes)==1:
                            #delete that table
                            print("deleting table: {}".format(row[0]))
                            with con:
                                con.execute("DROP TABLE {};".format(row[0]))
                        else:
                            only_good_scopes=([x for x in scopes if 'validation' not in x and 'test' not in x])
                            if len(only_good_scopes)==0:
                                #delete that table
                                print("deleting table: {}".format(row[0]))
                                with con:
                                    con.execute("DROP TABLE {};".format(row[0]))    
                            else:
                                print("deleting scopes from: {}".format(row[0]))
                                with con:
                                    con.execute("DELETE FROM {} WHERE scope is like validation;".format(row[0],scope))
                                    con.execute("DELETE FROM {} WHERE scope is like test;".format(row[0],scope))
                                    #"""
                            

                        
            
#con.execute('DELETE FROM {} WHERE scope = "{}";'.format(row[0],scope))
#con.commit()
                    
add_time(name)