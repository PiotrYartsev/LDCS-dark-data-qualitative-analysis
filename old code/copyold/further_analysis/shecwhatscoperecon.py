import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 

from subprocess import PIPE, Popen

name='Lund_GRID_all.db'
con = sl.connect('{}'.format(name))
#con=sl.connect("C:\\\\Users\\\\MSI PC\\\\Desktop\\\\gitproj\\\\LDCS-dark-data-qualitative-analysis\\\\{}".format(name))

isrecon={}
notrecon={}
nonrecon={}
for row in (con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        
        recon=con.execute('Select Scope from {} where IsRecon="True";'.format(row[0])).fetchall()
        if len(recon)==0:
            pass
        else:
            recon=[i[0].replace(" ","") for i in recon]
            print(row[0])
            for i in list(set(recon)):
                if i in isrecon:
                    isrecon[i]+=recon.count(i)
                else:
                    isrecon[i]=recon.count(i)
        not_recon=con.execute('Select Scope from {} where IsRecon="False";'.format(row[0])).fetchall()
        non_recon=[i[0].replace(" ","") for i in not_recon]
        for i in list(set(non_recon)):
            if i in notrecon:
                notrecon[i]+=non_recon.count(i)
            else:
                notrecon[i]=non_recon.count(i)

        non_recon=con.execute('Select Scope from {} where IsRecon!="True" or IsRecon!="False";'.format(row[0])).fetchall()
        non_recon=[i[0].replace(" ","") for i in non_recon]
        for i in list(set(non_recon)):
            if i in nonrecon:
                nonrecon[i]+=non_recon.count(i)
            else:
                nonrecon[i]=non_recon.count(i)
print(isrecon)
print(notrecon)
print(nonrecon)