"""
Caching functions and decorators

"""
# Copyright 2021 Benedat LLC
# Apache 2.0 license

import time
import os
from os.path import exists, abspath, expanduser, join, exists
import shutil
from typing import Optional
import json
import binascii

from joblib.memory import Memory
from s3fs import S3FileSystem
from joblib._store_backends import FileSystemStoreBackend, concurrency_safe_rename, concurrency_safe_write
from joblib.memory import register_store_backend

try:
    from .crypto import get_new_key, encrypted_file_open
except ImportError:
    # when running locally
    from crypto import get_new_key, encrypted_file_open


class CommandError(Exception):
    pass

def init_cache(cache_dir, max_size_in_mb:Optional[int]=None,
               _config_base_dir:Optional[str]=None):
    if _config_base_dir is None:
        # normally, we use the home directory.
        _config_base_dir = abspath(expanduser('~'))
    config_dir = join(_config_base_dir, '.dml')
    if not exists(config_dir):
        os.makedirs(config_dir)
    cfg_file = join(config_dir, 'config')
    cred_file = join(config_dir, 'credentials')
    if exists(cfg_file):
        raise CommandError(f"Config file {cfg_file} already exits. Remove it before re-initializing the configuration.")
    if exists(cred_file):
        raise CommandError(f"Credentials file {cred_file} already exits. Remove it before re-initializing the configuration.")
    key = get_new_key()
    with open(cfg_file, 'w') as f:
        json.dump({
            "cache_dir":cache_dir,
            "max_size_in_mb":max_size_in_mb
        }, f, indent=2)
    print(f"Wrote {cfg_file}")
    with os.fdopen(os.open(cred_file, os.O_CREAT|os.O_WRONLY, 0o600), 'w') as g:
        json.dump({
            'cache_keys': {
                'default':key
            }
        }, g, indent=2)
    print(f"Wrote {cred_file}")


class CacheConfigError(Exception):
    pass

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

WRITE_ID=0

class EncryptedStoreBackend(FileSystemStoreBackend):
    def __init__(self, *args, **kwargs):
        self._key = None
        super().__init__(*args, **kwargs)

    def _open_item(self, f, mode):
        assert self._key is not None
        return encrypted_file_open(f, mode, self._key)

    def _move_item(self, src, dest):
        concurrency_safe_rename(src, dest)
        #print(f"_move_item({src}, {dest})") # XXX

    def configure(self, location, verbose=1, backend_options=None):
        assert isinstance(backend_options, dict), f"Got {repr(backend_options)} for backend_options"
        print(f"configure({location}, verbose={verbose}, backend_options={backend_options})")
        self._key = backend_options['key']
        del backend_options['key']
        super().configure(location=location, verbose=verbose, backend_options=backend_options)

    # def _item_exists(self, location): # XXX
    #     r = super()._item_exists(location)
    #     print(f"_item_exists({location}) => {r}")
    #     return r

    def _concurrency_safe_write(self, to_write, filename, write_func):
        global WRITE_ID
        WRITE_ID += 1
        write_id = WRITE_ID
        #print(f"({write_id:03d})wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww")
        try:
            #print(f"({write_id:03d})_concurrency_safe_write(filename={filename})")
            temporary_filename = concurrency_safe_write(to_write,
                                                        filename, write_func)
            #print(f"  CSW({write_id:3d}): finished write, temp file is {temporary_filename}")
        except Exception as e:
            print(f"ERROR: ({write_id:03d}) concurrency_safe_write got an error: {e}", file=sys.stderr)
            raise
        self._move_item(temporary_filename, filename)
        #print(f"  CWS({write_id:3d}): moved item from {temporary_filename} to {filename}")
        #print(f"({write_id:03d})wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww")

    # def contains_item(self, path): # XXX
    #     r = super().contains_item(path)
    #     print(f"contains_item({path}) => {r}")
    #     return r

register_store_backend('encrypted', EncryptedStoreBackend)


class Cache(Memory):
    def __init__(self, encryption_key_name=None, verbose=1, _config_base_dir=None):
        """We read our parameters from the cache rather than from
        passed in parameters"""
        if _config_base_dir is None:
            _config_base_dir = abspath(expanduser('~'))
        config_dir = join(_config_base_dir, '.dml')
        cfg_file = join(config_dir, 'config')
        cred_file = join(config_dir, 'credentials')
        if not exists(cfg_file):
            raise CacheConfigError(f"Configuration file {cfg_file} not found. Did you initialize the cache?")
        if not exists(cred_file):
            raise CacheConfigError(f"Credentials file {cred_file} not found. Did you initialize the cache?")
        with open(cfg_file, 'r') as f:
            cfg_data = json.load(f)
        with open(cred_file, 'r') as g:
            cred_data = json.load(g)
        cache_dir = cfg_data['cache_dir']
        max_size_in_mb = cfg_data['max_size_in_mb']
        if not isinstance(max_size_in_mb, int) and (max_size_in_mb is not None):
            raise CachConfigError(f"Invalid value for max_size_in_mb: {repr(max_size_in_mb)}")
        bytes_limit = 1024*1024*max_size_in_mb if max_size_in_mb is not None \
                      else None
        if verbose>1:
            print(f"location={cache_dir}, bytes_limit={bytes_limit}, verbose={verbose}")
        if encryption_key_name is not None:
            cache_keys = cred_data['cache_keys']
            if encryption_key_name not in cache_keys:
                raise CacheConfigError(f"Did not find encryption key {encryption_key_name} in credentials file.")
            key = cache_keys[encryption_key_name]
            if verbose>1:
                print(f"Using encrypted backend, key {encryption_key_name}")
            super().__init__(location=cache_dir, bytes_limit=bytes_limit, backend='encrypted',
                             backend_options={'key':key}, verbose=verbose)
        else:
            super().__init__(location=cache_dir, bytes_limit=bytes_limit, verbose=verbose)



