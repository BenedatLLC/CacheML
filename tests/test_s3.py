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
from cacheml.cache import S3File

S3_PATH="s3://benedat-cache-test/commits.csv.gz"


EFFECTIVE_DATE = datetime.fromisoformat('2021-04-01')
if EFFECTIVE_DATE.tzinfo is None:
    EFFECTIVE_DATE = EFFECTIVE_DATE.replace(tzinfo=timezone.utc)
START_DATE=EFFECTIVE_DATE-relativedelta(years=25)

MIN_EXPECTED_FULL_READ_TIME=120
MAX_CACHE_READ_TIME=5

class TestS3File(unittest.TestCase):
    def setUp(self):
        clear_cache()
        self.memory = Memory(get_cache_path(), verbose=0)

    def test_df_caching(self):
        print("**** test_df_caching ****")
        # first see the time just to read the csv without writing to cache
        s3_file = S3File(S3_PATH)
        with s3_file.open('rb') as f:
            df_orig = timeit_with_range(self,MIN_EXPECTED_FULL_READ_TIME,
                                        None, pd.read_csv, f,
                                        header=0, converters={'commit_author_date':pd.to_datetime}, 
                                        usecols=[0,2,3,6,7,8],
                                        compression='gzip')
        print(df_orig.head())
        @self.memory.cache
        def my_read_csv(cache_file, usecols):
            with cache_file.open('rb') as f:
                return pd.read_csv(f, header=0, converters={'commit_author_date':pd.to_datetime},
                                   usecols=usecols, compression='gzip')
        df = timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None, my_read_csv, s3_file, [0,2,3,6,7,8])
        self.assertTrue(df.equals(df_orig))
        df = timeit_with_range(self, 0, MAX_CACHE_READ_TIME, my_read_csv, s3_file, [0,2,3,6,7,8])
        self.assertTrue(df.equals(df_orig))

    def test_read_commits_file_caching(self):
        cache_file = S3File(S3_PATH)
        ts_orig = timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None,
                                    read_commits_file, cache_file,
                                    usecols=[0,2,3,6,7,8], start=START_DATE, end=EFFECTIVE_DATE,
                                    lower_case=True)
        @self.memory.cache
        def my_read_commits(commits_file, start, end):
            return read_commits_file(commits_file, usecols=[0,2,3,6,7,8], start=start, end=end,
                                     lower_case=True)
        print("**** test_read_commits_file_caching ****")
        ts = timeit_with_range(self, MIN_EXPECTED_FULL_READ_TIME, None, my_read_commits, cache_file,
                               START_DATE, EFFECTIVE_DATE)
        self.assertTrue(ts.equals(ts_orig))
        ts = timeit_with_range(self, 0, MAX_CACHE_READ_TIME, my_read_commits, cache_file,
                               START_DATE, EFFECTIVE_DATE)
        self.assertTrue(ts.equals(ts_orig))

if __name__ == '__main__':
    unittest.main()
