"""
Caching functions and decorators
"""

import time
import os
from os.path import exists, abspath, expanduser

from s3fs import S3FileSystem

class CachedFile:
    __slots__=()
    def open(self, mode):
        raise NotImplemented()


class LocalFile(CachedFile):
    __slots__ = ('path', 'stats')
    def __init__(self, path):
        self.path = abspath(expanduser(path))
        self._refresh_stats()

    def _refresh_stats(self):
        fstats = os.stat(self.path)
        self.stats = (fstats.st_mtime, fstats.st_size)

    def __hash__(self):
        self._refresh_stats()
        hv = hash((self.path, self.stats[0], self.stats[1]),)
        #print(f"hash for {self} is {hv}")
        return hv

    def __getstate__(self):
        self._refresh_stats()
        #print(f"get_state, stats={self.stats}")
        return (self.path, self.stats[0], self.stats[1])

    def __setstate__(self, newstate):
        self.path = newstate[0]
        self.stats = (newstate[1], newstate[2])
        #print(f"set_state, stats={self.stats}")

    def open(self, mode):
        return open(self.path, mode)

    def __repr__(self):
        return f'LocalFile({self.path}, {self.stats})'


class S3File(CachedFile):
    __slots__ = ('fs', 'path', 'stats')


    def __init__(self, path):
        self.fs = S3FileSystem()
        self.path = path
        self._refresh_stats()
        print(self)

    def _refresh_stats(self):
        info = self.fs.info(self.path)
        self.stats = (info['LastModified'], info['size'])

    def __hash__(self):
        self._refresh_stats()
        hv = hash((self.path, self.stats[0], self.stats[1]),)
        print(f"hash for {self} is {hv}")
        return hv

    def __getstate__(self):
        self._refresh_stats()
        return (self.path, self.stats[0], self.stats[1])

    def __setstate__(self, newstate):
        self.path = newstate[0]
        self.stats = (newstate[1], newstate[2])
        self.fs = S3FileSystem()

    def open(self, mode):
        print(f"opening {self.path}")
        return self.fs.open(self.path, mode)

    def __repr__(self):
        return f'S3File({self.path}, {self.stats})'
