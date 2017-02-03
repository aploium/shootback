# shootback
shootback2's readme is still writing.......

## warning

1. in windows, due to the limit of CPython `select.select()`,
   shootback can NOT handle more than 512 concurrency, you will meet  
    `ValueError: too many file descriptors in select()`  
   If you have to handle such high concurrency in windows,
   [Anaconda-Python3](https://www.continuum.io/downloads) is recommend,
   [it's limit in windows is 2048](https://github.com/ContinuumIO/anaconda-issues/issues/1241)



