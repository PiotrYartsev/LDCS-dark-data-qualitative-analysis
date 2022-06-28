from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator

position=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93]
time_no_problem=[8743,8745,8746,8775,8761,8774,8783,8786,8799,8925,8833,8860,8854,8857,8864,8878,8887,8897,8898,8902,8913,8916,8930,8929,8949,8954,8962,8965,8977,8930,8959,8970,8563,8620,8683,8751,8806,8866,8928,9040,9166,9147,9287,9332,9402,9463,9529,9967,9708,9753,9836,9875,9965,10071,10131,10176,10265,10309,10367,10422,10490,10551,10606,10610,10681,10738,10843,10931,10974,11092,11178,11250,11311,11417,11343]



time_1=[8585,8581,8816,8849,8943,9022,9339,9271,9338,9431,9553,9584,9734,9973,9841,10252,10030,10113]
time_2=[8573,8573,8636,8605,8592,8607,8609,8624,8618,8646,8674,8670,8671,8673,8686,8676,8679,8688]

plt.plot(time_2,position[:len(time_1)],".",label="Early duplicate", markersize=10)
plt.plot(time_1,position[:len(time_1)],".",label="Later duplicate", markersize=10)
plt.plot(time_no_problem,position[len(time_1):],".",label="Not a duplicate", markersize=10)
plt.grid(linestyle='--',)
plt.title("Timestamp of file for early duplicate, late duplicate and not a duplicate file",fontsize=20)
plt.xlabel('Time',fontsize=15)
plt.ylabel('File number',   fontsize=15)
plt.legend(loc="upper right",fontsize=15)
plt.ylim(0)
plt.xlim(8500)
plt.show()