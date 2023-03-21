# Use this to see how much memory the dataframes use
from sys import getsizeof

def sizemb(obj, mib=False):
    """Use sys.getsizeof to return the memory usage of an object
    in megabytes (1 MB = 10**6 bytes).
    If mib=True, get memory usage in mebibytes instead
    (1 MiB = 2**20 bytes = 1.048576 MB).
    """
    mb = 2**20 if mib else 10**6
    return getsizeof(obj) / mb