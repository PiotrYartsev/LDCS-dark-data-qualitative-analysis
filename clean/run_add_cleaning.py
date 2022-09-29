import adjust_time
import add_duplicate_number
import fix_many_batches_in_one
import add_file_number
import cleanfilesinmanylocations
import os
import shutil

database='SLAC_mc20_2.db'

term_size = os.get_terminal_size()

print("\nAdjusting time")
print('=' * term_size.columns)
adjust_time.add_time(database)

print("\nRemoving duplicate batches")
print('=' * term_size.columns)
fix_many_batches_in_one.fix_many_batches_in_one(database)



print("\nAdding file number")
print('=' * term_size.columns)
add_file_number.add_file_number(database)

print("\nRemoving duplicate files")
print('=' * term_size.columns)
add_duplicate_number.add_duplicate_number(database)

print("\nAdding duplicate number")
shutil.copyfile(database, "{}_before_last_step.db".format(database[:-3]))

print('=' * term_size.columns)
add_duplicate_number.add_duplicate_number(database)

os.remove("{}_before_last_step.db".format(database[:-3]))