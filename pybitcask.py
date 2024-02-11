import os
import struct
import zlib

class pyBitcask:
    def __init__(self, db_path):
        self.db_path = db_path
        self.index = {}  # In-memory index
        self._load()

    def _serialize_entry(self, key, value):
        """Serialize key-value pair with CRC, lengths, and data."""
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')
        key_length = len(key_bytes)
        value_length = len(value_bytes)
        format_str = f'!I I I {key_length}s {value_length}s'
        entry = struct.pack(format_str, 0, key_length, value_length, key_bytes, value_bytes)
        crc = zlib.crc32(entry[4:])  # Calculate CRC excluding the CRC field itself
        entry = struct.pack('!I', crc) + entry[4:]
        return entry

    def _deserialize_entry(self, entry_bytes):
        """Deserialize entry into key, value, validating CRC."""
        crc, key_length, value_length = struct.unpack('!I I I', entry_bytes[:12])
        key, value = struct.unpack(f'!{key_length}s {value_length}s', entry_bytes[12:])
        calculated_crc = zlib.crc32(entry_bytes[4:])
        if crc != calculated_crc:
            raise ValueError("Data corruption detected")
        return key.decode('utf-8'), value.decode('utf-8')

    def _load(self):
        """Load existing data into the index."""
        if os.path.exists(self.db_path):
            with open(self.db_path, 'rb') as f:
                while True:
                    entry_header = f.read(12)  # Read CRC, key length, and value length
                    if not entry_header:
                        break
                    crc, key_length, value_length = struct.unpack('!I I I', entry_header)
                    entry_data = f.read(key_length + value_length)
                    entry = entry_header + entry_data
                    key, _ = self._deserialize_entry(entry)
                    pos = f.tell() - len(entry)
                    self.index[key] = pos

    def set(self, key, value):
        """Write a key-value pair."""
        entry = self._serialize_entry(key, value)
        with open(self.db_path, 'ab') as f:
            pos = f.tell()
            f.write(entry)
            self.index[key] = pos

    def get(self, key):
        """Read a value by key."""
        if key in self.index:
            with open(self.db_path, 'rb') as f:
                f.seek(self.index[key])
                entry_header = f.read(12)  # CRC, key length, and value length
                _, key_length, value_length = struct.unpack('!I I I', entry_header)
                entry_data = f.read(key_length + value_length)
                entry = entry_header + entry_data
                _, value = self._deserialize_entry(entry)
                return value
        return None
      
    def compact(self):
        """Compact the database file by removing deleted and obsolete entries."""
        new_db_path = f"{self.db_path}.tmp"
        new_index = {}
        with open(new_db_path, 'wb') as new_file:
            for key in sorted(self.index.keys()):
                value = self.get(key)  # This ensures we only get the latest value.
                if value is not None:  # Skip deleted keys.
                    entry = self._serialize_entry(key, value)
                    pos = new_file.tell()
                    new_file.write(entry)
                    new_index[key] = pos
        os.replace(new_db_path, self.db_path)
        self.index = new_index

