import matplotlib.pyplot as plt
import numpy as np


position=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]

time_1=[8585,8581,8816,8849,8943,9022,9339,9271,9338,9431,9553,9584,9734,9973,9841,10252,10030,10113,8743,8745,8746,8775,8761]
time_2=[8573,8573,8636,8605,8592,8607,8609,8624,8618,8646,8674,8670,8671,8673,8686,8676,8679,8688,8743,8745,8746,8775,8761]
time_no_problem=[]
for i in range(len(time_1)):
    output=abs(time_1[i]-time_2[i])
    time_no_problem.append(output)

plt.plot(position,time_no_problem,label="difference in time",linewidth=2)

plt.grid(linestyle='--')
plt.xlabel('File number',fontsize=15)
plt.ylabel("Time difference",   fontsize=15)
plt.title("Time difference between two duplicates",fontsize=20)

plt.legend(loc="upper right",fontsize=15)
plt.ylim(0)
plt.xlim(0)
plt.show()