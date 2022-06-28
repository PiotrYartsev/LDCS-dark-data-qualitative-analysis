from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime

import sqlite3 as sl

con = sl.connect('duplicate_data.db')

max_file_number = con.execute("""
SELECT MAX(file_number) FROM USER;""").fetchone()[0]
#print(max_file_number)



time_1_list=[]
time_2_list=[]
time_no_problem=[]
position=list(range(1,max_file_number+1))
for number in position:
    #print(int(number))
    creation_time = con.execute("""
    SELECT JobSubmissionTime FROM USER WHERE file_number LIKE ?;""", (int(number),)).fetchall()
    
    if len(creation_time)>1:
        time1=creation_time[0][0]
        time2=creation_time[1][0]
        if time1>time2:
            time_1_list.append(time2)
            time_2_list.append(time1)
        else:
            time_1_list.append(time1)
            time_2_list.append(time2)
    else:
        time_no_problem.append(creation_time[0][0])
        
creation_time_place_lund=con.execute("""
SELECT JobSubmissionTime,file_number FROM USER WHERE ComputingElement LIKE '%lunarc%';""", ()).fetchall()


creation_time_place_lund.sort(key=lambda x: x[1])
creation_time_place_list_lund=[]
creation_time_place_number_lund_list=[]
print(creation_time_place_lund[:5])
for n in range(len(creation_time_place_lund)):
    creation_time_place_list_lund.append(creation_time_place_lund[n][0])
    creation_time_place_number_lund_list.append(creation_time_place_lund[n][1])


creation_time_place_slac=con.execute("""
SELECT JobSubmissionTime,file_number FROM USER WHERE ComputingElement LIKE '%slac%';""", ()).fetchall()


creation_time_place_slac.sort(key=lambda x: x[1])
creation_time_place_list_slac=[]
creation_time_place_number_slac_list=[]
print(creation_time_place_slac[:5])
for n in range(len(creation_time_place_slac)):
    creation_time_place_list_slac.append(creation_time_place_slac[n][0])
    creation_time_place_number_slac_list.append(creation_time_place_slac[n][1])
print(creation_time_place_list_slac[:5])
print(len(creation_time_place_list_slac))
print(creation_time_place_number_slac_list[:5])
print(len(creation_time_place_number_slac_list))





plt.plot(creation_time_place_list_lund,creation_time_place_number_lund_list,"+",label="Created at Lund", markersize=10)
plt.plot(creation_time_place_list_slac,creation_time_place_number_slac_list,"o",label="Created at SLAC", markersize=10)

plt.plot(time_1_list,position[:len(time_1_list)],".",label="Early duplicate", markersize=10)
plt.plot(time_2_list,position[:len(time_1_list)],".",label="Later duplicate", markersize=10)
plt.plot(time_no_problem,position[len(time_1_list):],".",label="Not a duplicate", markersize=10)
plt.grid(linestyle='--',)
plt.title("Submission of file for early duplicate, late duplicate and not a duplicate file",fontsize=20)
plt.xlabel('Time',fontsize=15)
plt.ylabel('File number',   fontsize=15)
plt.legend(loc="upper right",fontsize=15)
plt.ylim(0)
#plt.xlim(8500)
plt.show()