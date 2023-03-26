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

class MappingViaAttributes(collections.abc.Mapping):
    """Implementation of the Mapping abstract base class that
    stores mapping keys as object attributes. This is a convenience
    whose main purpose is to allow tab completion of mapping keys
    in an interactive coding environment.
    """
    def __init__(self, mapping):
        """Create a MappingViaAttributes object from a dictionary
        or other implementation of Mapping that maps keys to values.
        Dictionary keys become object attributes that store the
        corresponding dictionary values.

        The keys in `mapping` must be valid Python variable names
        (this is not enforced in the constructor, but bad variable
        names will cause problems when trying to access the values
        via attributes).
        """
        self.__dict__ = dict(mapping)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def to_dict(self):
        return dict(self.__dict__)

    def keylist(self):
        return list(self.keys())
