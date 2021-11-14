import unittest
import sys
from os.path import join
import os
from datetime import datetime, timezone
import pickle
import time

from dateutil.relativedelta import relativedelta
import numpy as np

from utils_for_tests import *
sys.path.append(get_module_path())
from cacheml.crypto import get_new_key, encrypted_file_open
from cacheml.cache import LocalFile, init_cache, Cache

DEBUG=False

EFFECTIVE_DATE = datetime.fromisoformat('2021-04-01')
if EFFECTIVE_DATE.tzinfo is None:
    EFFECTIVE_DATE = EFFECTIVE_DATE.replace(tzinfo=timezone.utc)
START_DATE=EFFECTIVE_DATE-relativedelta(years=25)

MIN_EXPECTED_FULL_READ_TIME=120
MAX_CACHE_READ_TIME=25

test_string=\
"""this is a test of the encryption function. We want to validate
that we can write in encrypted form and read back as a decrypted file.
This data is converted to binary form before passing to the crypto
functions. We make sure we have more than a few 16-byte blocks.
"""

class TestFileReadWrite(unittest.TestCase):
    def setUp(self):
        clear_tempdir()
        os.mkdir(TEMPDIR)

    def tearDown(self):
        clear_tempdir(DEBUG)

    def test_write_and_read(self):
        key = get_new_key()
        data = test_string.encode('utf-8')
        filename = join(TEMPDIR, 'test_data.pkl')
        with encrypted_file_open(filename, 'wb', key) as f:
            cnt = f.write(data[0:-10])
            cnt += f.write(data[-10:])
            self.assertEqual(cnt, len(data))
        for buf_size in [4096, 256, 32, 31]:
            with encrypted_file_open(filename, 'rb', key, buf_size=buf_size) as f:
                read_bytes = f.read()
            read_string = read_bytes.decode('utf-8')
            self.assertEqual(test_string, read_string)

    def test_write_buffers(self):
        """Test writing with different sized bufferes"""
        key = get_new_key()
        data = test_string.encode('utf-8')
        filename = join(TEMPDIR, 'test_data.pkl')
        for buf_size in [4096, 256, 32, 31]:
            with encrypted_file_open(filename, 'wb', key, buf_size=buf_size) as f:
                cnt = f.write(data)
                self.assertEqual(cnt, len(data))
            with encrypted_file_open(filename, 'rb', key) as f:
                read_bytes = f.read()
            read_string = read_bytes.decode('utf-8')
            self.assertEqual(test_string, read_string)

    def test_readline(self):
        key = get_new_key()
        data = test_string.encode('utf-8')
        filename = join(TEMPDIR, 'test_data.pkl')
        with encrypted_file_open(filename, 'wb', key) as f:
            cnt = f.write(data[0:-10])
            cnt += f.write(data[-10:])
            self.assertEqual(cnt, len(data))
        for buf_size in [4096, 256, 32]:
            print(f"Testing readline() with buffer size of {buf_size} (data is {len(data)} bytes)")
            read_bytes = bytes()
            with encrypted_file_open(filename, 'rb', key, buf_size=buf_size) as f:
                while True:
                    line =  f.readline()
                    if len(line)==0:
                        break
                    print(repr(line))
                    self.assertTrue(line.endswith(b'\n'),
                                    f"Line does not end with newline: {line}")
                    read_bytes += line
            read_string = read_bytes.decode('utf-8')
            self.assertEqual(test_string, read_string)
            print(f"  Subtest for buffer size {buf_size} OK.")

    def test_with_pickle(self):
        key = get_new_key()
        data = np.ones(3)
        filename = join(TEMPDIR, 'test_data.pkl')
        with encrypted_file_open(filename, 'wb', key) as f:
            pickle.dump(data, f)
        with encrypted_file_open(filename, 'rb', key) as g:
            read_data = pickle.load(g)
        self.assertTrue((data==read_data).all())

    def test_with_pickle_large(self):
        key = get_new_key()
        data = np.arange(0, 4000000)
        filename = join(TEMPDIR, 'test_data.pkl')
        wr_start = time.time()
        with encrypted_file_open(filename, 'wb', key) as f:
            pickle.dump(data, f)
        wr_end = time.time()
        print(f"Time for write of 1M array is {round(1000*(wr_end-wr_start), 1)} miliseconds")
        for buf_size in [257, 64*4096, 64*1024*1024]:
            print(f"*********** Testing with Pickle, bufsize={buf_size}")
            with encrypted_file_open(filename, 'rb', key, buf_size=buf_size) as g:
                read_data = pickle.load(g)
            self.assertTrue((data==read_data).all())


class TestCachingWithEncryption(unittest.TestCase):
    def setUp(self):
        clear_cache()
        clear_tempdir()
        os.mkdir(TEMPDIR)
        init_cache(get_cache_path(), None, _config_base_dir=TEMPDIR)
        self.cache = Cache(encryption_key_name='default', _config_base_dir=TEMPDIR, verbose=2 if DEBUG else 1)

    def tearDown(self):
        clear_cache(DEBUG)
        clear_tempdir(DEBUG)

    def test_read_small_df(self):
        cache_file = LocalFile(get_small_local_data_file())
        @self.cache.cache
        def my_read_csv(cache_file, usecols):
            return pd.read_csv(cache_file.path, header=0, usecols=usecols)
        start = time.time()
        df_orig = my_read_csv(cache_file, [0,1,2])
        nocache_time = round(time.time() - start, 2)
        start = time.time()
        df = my_read_csv(cache_file, [0,1,2])
        cache_time = round(time.time() - start, 2)
        self.assertGreaterEqual(nocache_time, cache_time)
        self.assertTrue(df.equals(df_orig))
        touch(get_local_data_file())
        start = time.time
        df = my_read_csv(cache_file, [0,1,2])
        self.assertTrue(df.equals(df_orig))

    def test_read_df(self):
        cache_file = LocalFile(get_local_data_file())
        @self.cache.cache
        def my_read_csv(cache_file, usecols):
            return pd.read_csv(cache_file.path, header=0, converters={'commit_author_date':pd.to_datetime},
                               usecols=usecols)
        df_orig = timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None, my_read_csv, cache_file, [0,2,3,6,7,8])
        df = timeit_with_range(self, 0, MAX_CACHE_READ_TIME, my_read_csv, cache_file, [0,2,3,6,7,8])
        self.assertTrue(df.equals(df_orig))
        touch(get_local_data_file())
        df = timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None, my_read_csv, cache_file, [0,2,3,6,7,8])
        self.assertTrue(df.equals(df_orig))


if __name__ == '__main__':
    unittest.main()


