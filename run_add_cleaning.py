import adjust_time
import add_duplicate_number
import fix_many_batches_in_one
import add_file_number
import os
database='Lund_all copy 2.db'

term_size = os.get_terminal_size()

print("Adjusting time")
print('=' * term_size.columns)
adjust_time.add_time(database)

print("Removing duplicate batches")
print('=' * term_size.columns)
fix_many_batches_in_one.fix_many_batches_in_one(database)

print("Adding file number")
print('=' * term_size.columns)
add_file_number.add_file_number(database)

print("Adding duplicate number")
print('=' * term_size.columns)
add_duplicate_number.add_duplicate_number(database)