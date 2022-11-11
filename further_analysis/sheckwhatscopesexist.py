import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 

from subprocess import PIPE, Popen

name='/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/datasets/onlyfull/Lund_GRID_delete_onlyfull.db'
con = sl.connect('{}'.format(name))
#con=sl.connect("C:\\\\Users\\\\MSI PC\\\\Desktop\\\\gitproj\\\\LDCS-dark-data-qualitative-analysis\\\\{}".format(name))
 
isrecon={}
notrecon={}
nonrecon={}
for row in (con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        are_there_duplicate=con.execute("SELECT Count(*) from {} Where duplicate is not Null;".format(row[0])).fetchall()
        if are_there_duplicate[0][0]==0:
            pass
        else:
            print(row[0])
            get_scope_regular=con.execute("SELECT Scope from {} Where duplicate is Null;".format(row[0])).fetchall()
            get_scope_regular=[i[0].replace(" ","") for i in get_scope_regular]
            get_scop_duplicate=con.execute("SELECT Scope from {} Where duplicate is not Null;".format(row[0])).fetchall()
            get_scop_duplicate=[i[0].replace(" ","") for i in get_scop_duplicate]
            get_scop_duplicate=list(set(get_scop_duplicate))
            get_scope_regular=list(set(get_scope_regular))
            print(get_scop_duplicate)
            print(get_scope_regular)