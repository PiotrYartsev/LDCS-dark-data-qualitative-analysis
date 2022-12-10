import random
import tqdm as tqdm
import numpy as np

from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl

from sqlalchemy import column


def thirtyseven(number):
    #have one 38.7 % chance of returning 1
    i=random.randint(1,100)

    if i<(30):
        return(True)
    else:
        return(False)



stats={}

stuff=np.linspace(1,1000,100000)
#print(stuff)
while stats:
    b=True
    k=0
    while b==True:
        if thirtyseven(n)==True:
            if k in stats.keys():
                stats[k]+=1
            else:
                stats[k]=1
            k+=1
        else:
            b=False
print(stats)
    
plt.bar(stats.keys(),stats.values())
plt.show()


#plot a decay curve that starts at 8000 and decays to 0 in 19 steps
def decay_curve(x):
    return(8000*(0.95**x))

#plot it 
x=np.linspace(0,19,20)
y=decay_curve(x)
plt.plot(x,y)
plt.show()