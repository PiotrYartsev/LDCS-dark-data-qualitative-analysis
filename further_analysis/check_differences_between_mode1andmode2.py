import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 

from subprocess import PIPE, Popen
delete_all_name='Lund_all_fixed_delete_all.db'
delete_all = sl.connect('{}'.format(delete_all_name))



delete_some_name='/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/done fidex but not used database/{}'.format(delete_all_name.replace('all.db','some.db'))
delete_some = sl.connect('{}'.format(delete_some_name))




same=[]
diffrent=[]
missing_dataset=[]
for row in (delete_some.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        #check if table exists in delete_all
        if row[0] in [i[0] for i in delete_all.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()]:
            #check if length of table is the same
            if len(delete_some.execute("SELECT * from {};".format(row[0])).fetchall())==len(delete_all.execute("SELECT * from {};".format(row[0])).fetchall()):
                same.append(row[0])
            else:
                diffrent.append(row[0])
        else:
            missing_dataset.append(row[0])
print(delete_all_name)
print('same: ',len(same))
print('diffrent: ',len(diffrent))
if len(diffrent)>0:
    print(diffrent) 
print('missing_dataset: ',len(missing_dataset))
if len(missing_dataset)>0:
    print(missing_dataset)