#!/usr/bin/env python3
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import sys
from os.path import exists
import shutil
from pathlib import Path
import os

import pandas as pd
from joblib.memory import Memory

sys.path.append('../git_analytics_tools/code')
from utils import read_commits_file

from cache import *

def timeit(f, *args, **kwargs):
    st = time.time()
    rv = f(*args, **kwargs)
    et = time.time()
    print(f"Total time for {f.__name__}({args}, {kwargs}) was {round(et-st, 2)} seconds")

EFFECTIVE_DATE = datetime.fromisoformat('2021-04-01')
if EFFECTIVE_DATE.tzinfo is None:
    EFFECTIVE_DATE = EFFECTIVE_DATE.replace(tzinfo=timezone.utc)
START_DATE=EFFECTIVE_DATE-relativedelta(years=25)

COMMITS_FILE='commits.csv.gz'
CACHE_DIR='./cfiles'

# if exists(CACHE_DIR):
#     shutil.rmtree(CACHE_DIR)
#     print("Cleared cache")
memory = Memory(CACHE_DIR, verbose=0)

# read_commits_file(COMMITS_FILE, usecols=[0,2,3,6,7,8],
#                                  start=START_DATE,
# end=EFFECTIVE_DATE, lower_case=True)

def touch(fpath):
    assert exists(fpath)
    print(f"pre-modtime = {os.stat(fpath).st_mtime}")
    Path(fpath).touch(exist_ok=True)
    print(f"post-modtime = {os.stat(fpath).st_mtime}")


CACHE_FILE=LocalFile(COMMITS_FILE)

@memory.cache
def my_read_commits(commits_file, start, end):
    return read_commits_file(commits_file.path, usecols=[0,2,3,6,7,8], start=start, end=end,
                             lower_case=True)

print("first call...")
timeit(my_read_commits, CACHE_FILE, START_DATE, EFFECTIVE_DATE)

print("second_call...")
timeit(my_read_commits, CACHE_FILE, START_DATE, EFFECTIVE_DATE)
print(hash(CACHE_FILE))

touch(COMMITS_FILE)
print(hash(CACHE_FILE))
print("third_call, after touch...")
timeit(my_read_commits, CACHE_FILE, START_DATE, EFFECTIVE_DATE)

print("Fourth call, with different start_date")
timeit(my_read_commits, CACHE_FILE, START_DATE+relativedelta(years=5), EFFECTIVE_DATE)

@memory.cache
def my_read_csv(cache_file, usecols):
    return pd.read_csv(cache_file.path, header=0, converters={'commit_author_date':pd.to_datetime},
                       usecols=usecols)

print("Direct read, first call")
timeit(my_read_csv, CACHE_FILE, usecols=[0,2,3,6,7,8])

print("Direct read, second call")
timeit(my_read_csv, CACHE_FILE, usecols=[0,2,3,6,7,8])
