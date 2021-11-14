========
Cache ML
========

Cache ML -- layer on top of joblib to cache parsed datasets, dramatically reducing load
time of large data files. Also supports encryption at rest. Currently supported backends
are local filesystem and S3.

Example Usage
-------------
Here is an example from a Jupyter notebook::

  import pandas as pd
  from cacheml.cache import LocalFile, Cache
  cache = Cache()
  @cache.cache # this function's result will be cached
  def read_and_filter_commits(commits_file_obj):
      return pd.read_csv(commits_file_obj.path)
  ts_all = read_and_filter_commits(LocalFile(commits.csv.gz))

Performance Test Results
------------------------
There are from running the unit tests which simulate loading the time series data from
datahut.ai, which is in a 216MB compressed csv file. The first case just loads into
a dataframe, while the second case does some additional processing (sorting, removing
entries outside a time range).

.. list-table:: Caching results from unit test, raw dataframes
   :header-rows: 1

   * - File location
     - Time for raw df read
     - Time for initial read and caching of file
     - Time for cached read
   * - Local File
     - 134.0
     - 130.9
     - 0.41
   * - S3
     - 153.6
     - 144.6
     - 0.38

.. list-table:: Caching results from unit test, procesed dataframes
   :header-rows: 1

   * - File location
     - Time for original function
     - Time for initial read and caching of file
     - Time for cached read
   * - Local File
     - 139.6
     - 142.49
     - 1.04
   * - S3
     - 153.4
     - 155.8
     - 0.99

Copyright
---------
Copyright 2021 by Benedat LLC. Available under the Apache 2.0 license.

