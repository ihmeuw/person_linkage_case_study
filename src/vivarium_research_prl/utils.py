# Use this to see how much memory the dataframes use
from sys import getsizeof
import collections
import re


def sizemb(obj, mib=False):
    """Use sys.getsizeof to return the memory usage of an object
    in megabytes (1 MB = 10**6 bytes).
    If mib=True, get memory usage in mebibytes instead
    (1 MiB = 2**20 bytes = 1.048576 MB).
    """
    mb = 2**20 if mib else 10**6
    return getsizeof(obj) / mb

def convert_to_variable_name(string):
    """Converts a string to a valid Python variable.

    Strings of non-word characters (regex matchs \W+) are converted to '_',
    and '_' is appended to the beginning of the string if the string starts
    with a digit (regex matches ^(?=\d)).

    Solution copied from here:
    https://stackoverflow.com/questions/3303312/how-do-i-convert-a-string-to-a-valid-variable-name-in-python
    """
    return re.sub('\W+|^(?=\d)', '_', string)
