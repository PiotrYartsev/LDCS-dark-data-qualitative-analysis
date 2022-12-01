
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


name='Lund_all_fixed_delete_all.db'
con = sl.connect('{}'.format(name))
max_file_number_list=[]
procentage_of_duplicates=[]

for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        check_if_reconstructions=con.execute("SELECT COUNT (*) FROM "+row[0]+" WHERE IsRecon is 'True'").fetchone()
        check_if_reconstructions=check_if_reconstructions

 