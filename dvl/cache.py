"""
Caching functions and decorators
"""

import time
import os
from os.path import exists, abspath, expanduser


class CachedFile:
    __slots__=()
    def open(self, mode):
        raise NotImplemented()


class LocalFile(CachedFile):
    __slots__ = ('path', 'stats')
    def __init__(self, path):
        self.path = abspath(expanduser(path))
        self.stats = self._get_stats()

    def _get_stats(self):
        stats = os.stat(self.path)
        return (stats.st_mtime, stats.st_size)

    def __hash__(self):
        self.stats = self._get_stats()
        hv = hash((self.path, self.stats[0], self.stats[1]),)
        print(f"hash for {self} is {hv}")
        return hv

    def __getstate__(self):
        self._get_stats()
        return (self.path, self.stats[0], self.stats[1])

    def __setstate__(self, newstate):
        self.path = newstate[0]
        self.stats = (newstate[1], newstate[2])

    def open(self, mode):
        return open(self.path, mode)

    def __repr__(self):
        return f'LocalFile({self.path}, {self.stats})'


class S3File(CachedFile):
    __slots__ = ('path', 'stats')


