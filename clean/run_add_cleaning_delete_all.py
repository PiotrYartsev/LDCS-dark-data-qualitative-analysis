import os
import shutil

import add_duplicate_number
import add_file_number
import adjust_time
import cleanfilesinmanylocations
import delete_test_all
import fix_many_batches_in_one
import rename_tables
import remove_duplicate_non_duplicate
database='Lund_all_fixed_copy1.db'
#make a copy of the database
shutil.copy(database, database[:-9]+'_delete_all.db')
database=database[:-9]+'_delete_all.db'

term_size = os.get_terminal_size()

print("\nRenaming tables")
print('=' * term_size.columns)
rename_tables.change_name(database)

print("Removing test and validation data from database")
print('=' * term_size.columns)
delete_test_all.delete_test_all(database)


print("\nAdjusting time")
print('=' * term_size.columns)
adjust_time.add_time(database)

print("\nRemoving duplicate batches")
print('=' * term_size.columns)
fix_many_batches_in_one.fix_many_batches_in_one(database)

print("\nRemoving files that have been registered mulitple times, but that are not duplicates")
print('=' * term_size.columns)
remove_duplicate_non_duplicate.delte_copyies(database)

print("\nAdding file number")
print('=' * term_size.columns)
add_file_number.add_file_number(database)

print("\nAdding duplicate number")
print('=' * term_size.columns)
add_duplicate_number.add_duplicate_number(database)







database='Lund_GRIDFTP_all_fixed_copy1.db'
#make a copy of the database
shutil.copy(database, database[:-9]+'_delete_all.db')
database=database[:-9]+'_delete_all.db'

term_size = os.get_terminal_size()


print("\nRenaming tables")
print('=' * term_size.columns)
rename_tables.change_name(database)

print("Removing test and validation data from database")
print('=' * term_size.columns)
delete_test_all.delete_test_all(database)

print("\nAdjusting time")
print('=' * term_size.columns)
adjust_time.add_time(database)

print("\nRemoving duplicate batches")
print('=' * term_size.columns)
fix_many_batches_in_one.fix_many_batches_in_one(database)

print("\nRemoving files that have been registered mulitple times, but that are not duplicates")
print('=' * term_size.columns)
remove_duplicate_non_duplicate.delte_copyies(database)

print("\nAdding file number")
print('=' * term_size.columns)
add_file_number.add_file_number(database)

print("\nAdding duplicate number")
print('=' * term_size.columns)
add_duplicate_number.add_duplicate_number(database)



