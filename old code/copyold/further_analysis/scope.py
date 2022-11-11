import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
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

from subprocess import PIPE, Popen

name='Lund_all.db'
con = sl.connect('/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/datasets/everything/{}'.format(name))
#con=sl.connect("C:\\\\Users\\\\MSI PC\\\\Desktop\\\\gitproj\\\\LDCS-dark-data-qualitative-analysis\\\\{}".format(name))





all_scopes2={}
duplicates_scopes={}

for row in tqdm((con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall())):
    if row[0] == 'sqlite_sequence':
        pass
    else:
        all_scopes=con.execute("SELECT scope FROM {}".format(row[0])).fetchall()
        
        all_scopes=[a[0] for a in all_scopes]
        all_scopes=[a.replace(" ","") for a in all_scopes]

        scopes_for_duplicates=con.execute("SELECT scope FROM {} where duplicate is not Null".format(row[0])).fetchall()
        
        scopes_for_duplicates=[a[0] for a in 
        scopes_for_duplicates]
        scopes_for_duplicates=[a.replace(" ","") for a in scopes_for_duplicates]
        for scope in scopes_for_duplicates:
            if scope in duplicates_scopes:
                duplicates_scopes[scope]+=1
            else:
                duplicates_scopes[scope]=1
        for scope in all_scopes:
            if scope in all_scopes2:
                all_scopes2[scope]+=1
            else:
                all_scopes2[scope]=1
print(duplicates_scopes)
print(all_scopes2)
procentage={}
for scope in all_scopes2:
    if scope in duplicates_scopes:
        procentage[scope]=round(100*duplicates_scopes[scope]/(all_scopes2[scope]+duplicates_scopes[scope]),2)
    else:
        pass
        #procentage[scope]=0
print(procentage)



#plot a bar chart
from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from tqdm import *
name2=name.replace(".db","")
name2=name2.replace("_all","")
name2=name2.replace("_2","")
name2=name2.replace("_"," ")
plt.bar(range(len(duplicates_scopes)), list(duplicates_scopes.values()), align='center')
plt.xticks(range(len(duplicates_scopes)), list(duplicates_scopes.keys()),rotation=90)
plt.ylabel('Number of files')
plt.title('{}: Number of files with different metadata'.format(name2))
plt.tight_layout(rect=[0,0.03,1,1])
name2=name.replace(".db","")
#plt.show()
plt.savefig('/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/figures/{}/bar-plot/{}_scopes.png'.format(name2,name2),bbox_inches='tight',dpi=300)
plt.close()





name2=name.replace(".db","")
name2=name2.replace("_all","")
name2=name2.replace("_2","")
name2=name2.replace("_"," ")
plt.bar(range(len(procentage)), list(procentage.values()), align='center')
plt.xticks(range(len(procentage)), list(procentage.keys()),rotation=90)
plt.ylabel('Number of files')
plt.title('{}: Number of files with different metadata'.format(name2))
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter())
plt.tight_layout(rect=[0,0.03,1,1])
name2=name.replace(".db","")
#plt.show()
plt.savefig('/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/figures/{}/bar-plot/{}_scopes_procentage.png'.format(name2,name2),bbox_inches='tight',dpi=300)






#"""/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/figures/Lund_all/bar-plot