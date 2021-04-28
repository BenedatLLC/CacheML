"""Profiling
and performance testing
"""
import sys
import os
from os.path import join
import pickle

import pandas as pd
import joblib

from utils_for_tests import *
sys.path.append(get_module_path())
from directml.crypto import *

KEY=get_new_key()

def my_read_csv(file_path, usecols=[0,2,3,6,7,8]):
    print("loading csv file")
    return pd.read_csv(file_path, header=0, usecols=usecols,
                       converters={'commit_author_date':pd.to_datetime})

def pickle_df(df):
    with encrypted_file_open(join(TEMPDIR, 'data.pkl'), 'wb', KEY) as f:
        pickle.dump(df, f)

def unpickle_df():
    with encrypted_file_open(join(TEMPDIR, 'data.pkl'), 'rb', KEY) as f:
        return pickle.load(f)

def jl_dump_df(df):
    with encrypted_file_open(join(TEMPDIR, 'data.pkl'), 'wb', KEY) as f:
        joblib.dump(df, f)

def jl_load_df():
    with encrypted_file_open(join(TEMPDIR, 'data.pkl'), 'rb', KEY) as f:
        return joblib.load(f)

def jl_cleartext_dump_df(df):
    with open(join(TEMPDIR, 'data.pkl'), 'wb') as f:
        joblib.dump(df, f)

def jl_cleartext_load_df():
    with open(join(TEMPDIR, 'data.pkl'), 'rb') as f:
        return joblib.load(f)

def run_encrypted_pickle():
    t1 = time.time()
    df = my_read_csv(get_local_data_file())
    print(f"Read csv in {fmt_time(t1)}")
    t2 = time.time()
    pickle_df(df)
    print(f"Pickle in {fmt_time(t2)}")
    t3 = time.time()
    df2 = unpickle_df()
    print(f"Unpickle in {fmt_time(t3)}")
    assert df2.equals(df)
    print(f"data frame has {len(df2.columns)} and {len(df2)} rows")

def run_encrypted_joblib_dl():
    t1 = time.time()
    df = my_read_csv(get_local_data_file())
    print(f"Read csv in {fmt_time(t1)}")
    t2 = time.time()
    jl_dump_df(df)
    print(f"Dump in {fmt_time(t2)}")
    t3 = time.time()
    df2 = jl_load_df()
    print(f"Load in {fmt_time(t3)}")
    assert df2.equals(df)
    print(f"data frame has {len(df2.columns)} and {len(df2)} rows")

def run_joblib_dl():
    t1 = time.time()
    df = my_read_csv(get_local_data_file())
    print(f"Read csv in {fmt_time(t1)}")
    t2 = time.time()
    jl_cleartext_dump_df(df)
    print(f"Dump in {fmt_time(t2)}")
    t3 = time.time()
    df2 = jl_cleartext_load_df()
    print(f"Load in {fmt_time(t3)}")
    assert df2.equals(df)
    print(f"data frame has {len(df2.columns)} and {len(df2)} rows")


def main(argv=sys.argv):
    clear_tempdir()
    os.mkdir(TEMPDIR)
    try:
        #run_encrypted_pickle()
        run_encrypted_joblib_dl()
        return 0
    finally:
        clear_tempdir()


if __name__=='__main__':
    sys.exit(main())

