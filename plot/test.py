import os
import sqlite3 as sl
from datetime import datetime
from tokenize import Number
import matplotlib.ticker as mtick

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axis import YAxis
from matplotlib.ticker import MultipleLocator
from sqlalchemy import column

name='Lund_GRIDFTP_all_fixed_delete_all.db'
con = sl.connect(name)
duplicates={}
non_duplicates={}
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        duplicate=con.execute('SELECT ComputingElement FROM {} where duplicate is not Null;'.format(row[0])).fetchall()
        duplicate=[x[0] for x in duplicate]
        #rename none to "none"
        duplicate=[x if x is not None else "none" for x in duplicate]
        duplicate=[x.replace(" ","") for x in duplicate]

        non_duplicate=con.execute('SELECT ComputingElement FROM {} where duplicate is Null;'.format(row[0])).fetchall()
        non_duplicate=[x[0]for x in non_duplicate]
        non_duplicate=[x if x is not None else "none" for x in non_duplicate]
        non_duplicate=[x.replace(" ","") for x in non_duplicate]

        for element in list(set(duplicate)):
            print(element)
            if element not in duplicates:
                duplicates[element]=duplicate.count(element)
            else:   
                duplicates[element]+=duplicate.count(element)
        for element in list(set(non_duplicate)):
            print(element)
            if element not in non_duplicates:
                non_duplicates[element]=non_duplicate.count(element)
            else:   
                non_duplicates[element]+=non_duplicate.count(element)
print(duplicates)
print(non_duplicates)