"""
disk_store module implements DiskStorage class which implements the KV store on the
disk

DiskStorage provides two simple operations to get and set key value pairs. Both key and
value needs to be of string type. All the data is persisted to disk. During startup,
DiskStorage loads all the existing KV pair metadata.  It will throw an error if the
file is invalid or corrupt.

Do note that if the database file is large, then the initialisation will take time
accordingly. The initialisation is also a blocking operation, till it is completed
the DB cannot be used.

Typical usage example:

    disk: DiskStorage = DiskStore(file_name="books.db")
    disk.set(key="othello", value="shakespeare")
    author: str = disk.get("othello")
    # it also supports dictionary style API too:
    disk["hamlet"] = "shakespeare"
"""
import datetime
import os.path
import time
import typing

from format import encode_kv, decode_kv, decode_header


# DiskStorage is a Log-Structured Hash Table as described in the BitCask paper. We
# keep appending the data to a file, like a log. DiskStorage maintains an in-memory
# hash table called KeyDir, which keeps the row's location on the disk.
#
# The idea is simple yet brilliant:
#   - Write the record to the disk
#   - Update the internal hash table to point to that byte offset
#   - Whenever we get a read request, check the internal hash table for the address,
#       fetch that and return
#
# KeyDir does not store values, only their locations.
#
# The above approach solves a lot of problems:
#   - Writes are insanely fast since you are just appending to the file
#   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
#       storage, there could be 2-3 disk seeks
#
# However, there are drawbacks too:
#   - We need to maintain an in-memory hash table KeyDir. A database with a large
#       number of keys would require more RAM
#   - Since we need to build the KeyDir at initialisation, it will affect the startup
#       time too
#   - Deleted keys need to be purged from the file to reduce the file size
#
# Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf


def read_serialised_header(a_file):
    header_buf = a_file.read(12)
    if len(header_buf) < 12:
        return None
    timestamp, key_size, value_size = decode_header(header_buf)
    return header_buf, timestamp, key_size, value_size


class DiskStorage:
    """
    Implements the KV store on the disk

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    def __init__(self, file_name: str = "data.db"):
        self.file_name = file_name
        self.mapping = dict()
        self.header_buf = None
        if os.path.exists(file_name):
            self._load_existing()
        self.writer = open(file_name, "ab")
        self.writer.seek(0, 2)  # SEEK_END = 2

    def _load_existing(self):
        with open(self.file_name, "rb") as a_file:
            while True:
                offset = a_file.tell()
                header = read_serialised_header(a_file)
                if header is None:
                    break
                header_buf, timestamp, key_size, value_size = header
                tail_buf = a_file.read(key_size + value_size)
                timestamp, key, value = decode_kv(header_buf + tail_buf)
                self.mapping[key] = offset

    def set(self, key: str, value: str) -> None:
        timestamp = int(datetime.datetime.now().timestamp())
        buf_size, buf = encode_kv(timestamp, key, value)
        offset = self.writer.tell()
        self.writer.write(buf)
        self.writer.flush()
        self.mapping[key] = offset

    def get(self, key: str) -> str:
        if key not in self.mapping:
            return ""
        offset = self.mapping[key]
        with open(self.file_name, "rb") as a_file:
            a_file.seek(offset)
            header = read_serialised_header(a_file)
            assert header is not None
            header_buf, timestamp, key_size, value_size = header
            tail_buf = a_file.read(key_size + value_size)
            timestamp, key, value = decode_kv(header_buf + tail_buf)
            return value

    def close(self) -> None:
        self.writer.close()

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)
