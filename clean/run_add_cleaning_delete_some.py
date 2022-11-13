import os
import shutil

import add_duplicate_number
import add_file_number
import adjust_time
import cleanfilesinmanylocations
import delete_test_some
import fix_many_batches_in_one

database='Lund_all.db'
#make a copy of the database
shutil.copy(database, database[:-3]+'_delete_some.db')
database=database[:-3]+'_delete_some.db'

term_size = os.get_terminal_size()

print("Removing test and validation data from database")
print('=' * term_size.columns)
delete_test_some.delete_test_some(database)

print("\nAdjusting time")
print('=' * term_size.columns)
adjust_time.add_time(database)

print("\nRemoving duplicate batches")
print('=' * term_size.columns)
fix_many_batches_in_one.fix_many_batches_in_one(database)

print("\nAdding file number")
print('=' * term_size.columns)
add_file_number.add_file_number(database)

print("\nAdding duplicate number")
print('=' * term_size.columns)
add_duplicate_number.add_duplicate_number(database)