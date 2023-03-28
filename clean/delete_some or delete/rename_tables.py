
from tqdm import tqdm
import sqlite3 as sl


def change_name(dataset):
    con = sl.connect(dataset, check_same_thread=False)

    #Retrive all tables from the database
    all_batches=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    all_batches=[a[0] for a in all_batches]
    for row in tqdm(all_batches):
        #If thea table is the table of tables, skip it
        if row == 'sqlite_sequence':
            pass
        else:

            if "_" in row:
                batches=con.execute("Select BatchID from {};".format(row)).fetchall()
                batches=[a[0] for a in batches if a[0] != None]
                batches=list(set(batches))
                if len(batches)==1:
                    batches=batches[0]
                    batches=''.join(e for e in batches if e.isalnum())
                    
                    if batches[0].isnumeric():
                        batches="B"+batches
                    
                    print(batches)
                    print("          "+"Tho old name is: "+row+"\n")
                    #change the name of the table
                    try:
                        
                        con.execute("ALTER TABLE {} RENAME TO {};".format(row,batches))
                        print("          "+"The new name is: "+str(batches)+"\n")
                    except:
                        
                        #find how many ocourrences of the name there are
                        count=con.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE '{}%';".format(batches)).fetchall()
                        count=count[0][0]
                        #add the number of ocourrences to the name
                        batches=batches+str(count+1)
                        print("          "+"The new name is: "+str(batches)+"\n")
                        con.execute("ALTER TABLE {} RENAME TO {};".format(row,batches))
