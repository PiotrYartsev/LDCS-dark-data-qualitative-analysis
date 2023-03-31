time_regular=open('C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\time_reg.txt','r')
time_regular=time_regular.read()
time_regular=time_regular.split('\n')

time_dup=open('C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\time_dup.txt','r')
time_dup=time_dup.read()
time_dup=time_dup.split('\n')

time_dup_in_duplicate='C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\time_of_duplciates_in_rucio.txt'
time_dup_in_duplicate=open(time_dup_in_duplicate,'r')
time_dup_in_duplicate=time_dup_in_duplicate.read()
time_dup_in_duplicate=time_dup_in_duplicate.split('\n')

time_commits='C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\GITHUB_timescale.txt'
time_commits=open(time_commits,'r')
time_commits=time_commits.read()
time_commits=time_commits.split('\n')

import matplotlib.pyplot as plt
import datetime

#remove empty strings
time_regular=[x for x in time_regular if x != '']
time_dup=[x for x in time_dup if x != '']
time_dup_in_duplicate=[x for x in time_dup_in_duplicate if x != '']
time_commits=[x for x in time_commits if x != '']

#convert them from unix time to datetime
time_regular=[datetime.datetime.fromtimestamp(int(x)) for x in time_regular]
#print(len(time_regular))
time_dup=[datetime.datetime.fromtimestamp(int(x)) for x in time_dup]
#print(len(time_dup))
time_dup_in_duplicate=[datetime.datetime.fromtimestamp(int(x)) for x in time_dup_in_duplicate]
time_commits=[datetime.datetime.fromtimestamp(int(x)) for x in time_commits]

#make a timeline of time_regular of these with lines for first and last, and add histogram for the rest
#make a timeline of time_dup of these with lines for first and last, and add histogram for the rest
#make a timeline of time_dup_in_duplicate of these with lines for first and last, and add histogram for the rest
#make a timeline of time_commits of these with lines for first and last, and add histogram for the rest

plt.plot(time_regular, [1]*len(time_regular), 'o', color='blue', label='Regular')
#plot a virtical line for the first and last
plt.plot([max(time_regular),max(time_regular)], [0,4], '-', color='blue')
plt.plot([min(time_regular),min(time_regular)], [0,4], '-', color='blue')

plt.plot(time_dup, [2]*len(time_dup), 'o', color='red', label='Duplicate')
plt.plot([max(time_dup),max(time_dup)], [0,4], '-', color='red')
plt.plot([min(time_dup),min(time_dup)], [0,4], '-', color='red')

plt.plot(time_dup_in_duplicate, [3]*len(time_dup_in_duplicate), 'o', color='green', label='Duplicate in Rucio')
plt.plot([max(time_dup_in_duplicate),max(time_dup_in_duplicate)], [0,4], '-', color='green')
plt.plot([min(time_dup_in_duplicate),min(time_dup_in_duplicate)], [0,4], '-', color='green')


plt.plot(time_commits, [4]*len(time_commits), 'o', color='black', label='Commit')
plt.plot([max(time_commits),max(time_commits)], [0,4], '-', color='black')
plt.plot([min(time_commits),min(time_commits)], [0,4], '-', color='black')

plt.legend(loc='upper left')
#make the plot to have a 10:1 x:y ratio but keep the y axis the same
fig=plt.gcf()
fig.set_size_inches(30,10)
fig.savefig('C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\time_evolution.png', bbox_inches='tight', dpi=300)



