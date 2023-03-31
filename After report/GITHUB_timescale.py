
import git

repo = git.Repo('C:\\Users\\piotr\\Desktop\\LDCSL')
commits = list(repo.iter_commits('master'))

for commit in commits:
    print(commit.committed_date)

#save to txt file to C:\Users\piotr\Documents\GitHub\LDCS-dark-data-qualitative-analysis

file=open('C:\\Users\\piotr\\Documents\\GitHub\\LDCS-dark-data-qualitative-analysis\\GITHUB_timescale.txt','w')
for commit in commits:
    file.write(str(commit.committed_date)+'\n')
file.close()
