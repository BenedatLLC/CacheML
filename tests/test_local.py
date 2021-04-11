#!/usr/bin/env python3
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import sys
from os.path import exists
import shutil
from pathlib import Path
import os
import unittest

import pandas as pd
from joblib.memory import Memory

from utils_for_tests import *
sys.path.append(get_module_path())
from dvl.cache import LocalFile


def touch(fpath):
    assert exists(fpath)
    print(f"pre-modtime = {os.stat(fpath).st_mtime}")
    Path(fpath).touch(exist_ok=True)
    print(f"post-modtime = {os.stat(fpath).st_mtime}")



EFFECTIVE_DATE = datetime.fromisoformat('2021-04-01')
if EFFECTIVE_DATE.tzinfo is None:
    EFFECTIVE_DATE = EFFECTIVE_DATE.replace(tzinfo=timezone.utc)
START_DATE=EFFECTIVE_DATE-relativedelta(years=25)

MIN_EXPECTED_FULL_READ_TIME=120
MAX_CACHE_READ_TIME=5

class TestLocalFile(unittest.TestCase):
    def setUp(self):
        clear_cache()
        self.memory = Memory(get_cache_path(), verbose=0)

    def test_df_caching(self):
        print("**** test_df_caching ****")
        # first see the time just to read the csv without writing to cache
        df_orig = timeit_with_range(self,MIN_EXPECTED_FULL_READ_TIME, None, pd.read_csv, get_local_data_file(),
                                    header=0, converters={'commit_author_date':pd.to_datetime}, usecols=[0,2,3,6,7,8])
        cache_file = LocalFile(get_local_data_file())
        @self.memory.cache
        def my_read_csv(cache_file, usecols):
            return pd.read_csv(cache_file.path, header=0, converters={'commit_author_date':pd.to_datetime},
                               usecols=usecols)
        df = timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None, my_read_csv, cache_file, [0,2,3,6,7,8])
        self.assertTrue(df.equals(df_orig))
        df = timeit_with_range(self, 0, MAX_CACHE_READ_TIME, my_read_csv, cache_file, [0,2,3,6,7,8])
        self.assertTrue(df.equals(df_orig))
        touch(get_local_data_file())
        df = timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None, my_read_csv, cache_file, [0,2,3,6,7,8])
        self.assertTrue(df.equals(df_orig))

    def test_read_commits_file_caching(self):
        cache_file = LocalFile(get_local_data_file())
        @self.memory.cache
        def my_read_commits(commits_file, start, end):
            return read_commits_file(commits_file, usecols=[0,2,3,6,7,8], start=start, end=end,
                                     lower_case=True)
        print("**** test_read_commits_file_caching ****")
        timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None, my_read_commits, cache_file,
                          START_DATE, EFFECTIVE_DATE)
        timeit_with_range(self, 0, MAX_CACHE_READ_TIME, my_read_commits, cache_file,
                          START_DATE, EFFECTIVE_DATE)
        touch(get_local_data_file())
        timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None, my_read_commits, cache_file,
                          START_DATE, EFFECTIVE_DATE)

if __name__ == '__main__':
    unittest.main()
