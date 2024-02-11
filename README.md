# PyBitcask
This project provides a Python implementation of a Bitcask-style key-value store, optimized for fast reads and writes.

##Usage
'''
from bitcask_kvstore import EnhancedBitcask

# Initialize the database
db = pyBitcask('path/to/db_directory')

# Set a key-value pair
db.set('hello', 'world')

# Retrieve a value
print(db.get('hello'))  # Output: 'world'

# Compact the database
db.compact()
'''
