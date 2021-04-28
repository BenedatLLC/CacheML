#!/usr/bin/env python3
import time
import os
from os.path import dirname, abspath, expanduser, join, exists
import shutil
from typing import Optional, List
from datetime import datetime
import pandas as pd
import pandas.core.dtypes
from pathlib import Path

def timeit(f, *args, **kwargs):
    st = time.time()
    rv = f(*args, **kwargs)
    et = time.time()
    print(f"Total time for {f.__name__}({args}, {kwargs}) was {round(et-st, 2)} seconds")

def timeit_with_range(self, min_time_secs, max_time_secs, f, *args, **kwargs):
    st = time.time()
    rv = f(*args, **kwargs)
    et = time.time()
    elapsed = et-st
    print(f"Total time for {f.__name__} was {round(elapsed, 2)} seconds")
    if min_time_secs is not None:
        self.assertGreaterEqual(elapsed, min_time_secs)
    if max_time_secs is not None:
        self.assertLessEqual(elapsed, max_time_secs)
    return rv

def fmt_time(start_time):
    """Format the time since the start time in either seconds or milliseconds"""
    elapsed = time.time() - start_time
    if elapsed < 1.0:
        return f"{round(1000*elapsed,1)} milliseconds"
    else:
        return f"{round(elapsed, 1)} seconds"


def get_this_dir():
    return abspath(expanduser(dirname(__file__)))

def get_module_path():
    return abspath(join(get_this_dir(), '..'))

def get_cache_path():
    return join(get_module_path(), 'test_cache')

TEMPDIR = join(abspath(expanduser(dirname(__file__))), 'test_temp')


def clear_cache(DEBUG=False):
    cpath = get_cache_path()
    if exists(cpath):
        if not DEBUG:
            shutil.rmtree(cpath)
        else:
            print(f"Skipping clear of cache at {cpath}, as DEBUG is True")

def clear_tempdir(DEBUG=False):
    if exists(TEMPDIR):
        if not DEBUG:
            shutil.rmtree(TEMPDIR)
        else:
            print(f"Skipping removal of TEMPDIR at {TEMPDIR}, as DEBUG is True")

def get_local_data_file():
    return join(get_module_path(), 'test_data/commits.csv.gz')

def get_small_local_data_file():
    return join(get_module_path(), 'test_data/small.csv.gz')


def touch(fpath):
    assert exists(fpath)
    print(f"pre-modtime = {os.stat(fpath).st_mtime}")
    Path(fpath).touch(exist_ok=True)
    print(f"post-modtime = {os.stat(fpath).st_mtime}")



# Code from git_analytics_tools.
# We include here to prevent circular dependencies

class TimeRange:
    """A time range is a continuous interval of time from the start timestamp,
    inclusive through the end timestamp, exclusive. Times must be timezone-aware.
    """
    __slots__ = ('start', 'end')
    def __init__(self, start:datetime, end:datetime):
        self.start = start
        self.end = end
        assert self.start.tzinfo is not None, "Start time not timezone-aware"
        assert self.end.tzinfo is not None, "End time not timezone aware"
        assert self.start<self.end, "End must be greater than start"

    def __eq__(self, other) -> bool:
        if not isinstance(other, TimeRange):
            return False
        else:
            return self.start==other.start and self.end==other.end

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __gt__(self, other) -> bool:
        if not isinstance(other, TimeRange):
            return False
        else:
            return (self.start > other.start) and (self.end > other.end)


    def __lt__(self, other) -> bool:
        if not isinstance(other, TimeRange):
            return False
        else:
            return (self.start < other.start) and (self.end < other.end)

    def __ge__(self, other) -> bool:
        return self==other or self>other

    def __le__(self, other) -> bool:
        return self==other or self<other


class TimeSeries:
    """A sequence of events with an associated timestamp.
    Either the timestamp is the index or a specified column.
    """
    def __init__(self, df:pd.DataFrame, ts_column:str):
        assert ts_column in df.columns, "%s is not a column in dataframe" % ts_column
        assert isinstance(df[ts_column].dtype, pandas.core.dtypes.dtypes.DatetimeTZDtype),\
             "%s should be of type pandas.core.dtypes.dtypes.DatetimeTZDtype"%ts_column
        self.df = df
        self.ts_column = ts_column
        self.ascending = None
        self.time_range = None

    def equals(self, other):
        """Return true if two time series are equal"""
        return self.df.equals(other.df) and self.ts_column==other.ts_column and self.ascending==other.ascending \
            and self.time_range==other.time_range

    def sort_in_place(self, ascending:bool=True) -> None:
        self.df.sort_values(by=self.ts_column, inplace=True, ascending=ascending)
        self.df.reset_index(drop=True, inplace=True)
        self.ascending = ascending

    def filter_time_range(self, time_range:TimeRange) -> 'TimeSeries':
        """Return a copy where any rows before the start time or equal to or after the end
        time have been dropped.
        """
        assert self.time_range is None
        # cannot use df.where(), as that may change the types of the other columns!
        ddf = self.df[(self.df[self.ts_column] < time_range.end) &
                      (self.df[self.ts_column] >= time_range.start)]
        ddf = ddf.dropna(axis=0, how='all', inplace=False)
        ts = self._make_return_copy(ddf, ts_column=self.ts_column)
        ts.time_range = time_range
        return ts

    def _make_return_copy(self, new_df:pd.DataFrame, ts_column) -> 'TimeSeries':
        # Internal method used to create a new time series with the new
        # dataframe and all the attributes set to the same value as this one.
        # The calling method can then overwrite specific attributes as needed.
        ts = TimeSeries(new_df, ts_column=ts_column)
        ts.ascending = self.ascending
        ts.time_range = self.time_range
        return ts



def read_commits_file(cached_file, usecols:List[int],
                      start:datetime, end:datetime,
                      lower_case:bool=False, compression:Optional[str]='infer') \
    -> TimeSeries:
    """Read the file and return a TimeSeries. File should be csv (.csv) or a
    gzipped csv (.csv.gz). The rows will be sorted by timestamp ascending and filtered
    within the time range.
    """
    assert end.day==1, "Invalid end date '%s' -  must be at the beginning of a month"%end
    if compression=='infer':
        if cached_file.path.endswith('.csv'):
            compression=None
        elif cached_file.path.endswith('.csv.gz'):
            compression = 'gzip'
        elif cached_file.path.endswith('.csv.bz2'):
            compresssion = 'bz2'
        elif cached_file.path.endswith('.csv.zip'):
            compresssion = 'zip'
        elif cached_file.path.endswith('.csv.xz'):
            compresssion = 'xz'
        else:
            assert f"Unabled to determine compression for {cached_file.path}"
    start_time = time.time()
    try:
        with cached_file.open('rb') as f:
            df = pd.read_csv(f, header=0, converters={'commit_author_date':pd.to_datetime},
                             usecols=usecols, compression=compression)
    except pd.errors.ParserError as e:
        print("Got a parser error, will retry with python engine: %s"%e, file=sys.stderr)
        with cached_file.open('rb') as f:
            df = pd.read_csv(f, header=0, converters={'commit_author_date':pd.to_datetime},
                             usecols=usecols, engine='python', compression=compression)
    df['repo'] = df['repo'].apply(lambda s:s.lower())
    ts = TimeSeries(df, ts_column='commit_author_date')
    ts.sort_in_place(ascending=True)
    end_time = time.time()
    print(f"Read and sort of dataframe took {round(end_time-start_time, 1)} seconds")
    return ts.filter_time_range(TimeRange(start, end))

