# Use this to see how much memory the dataframes use
from sys import getsizeof
import collections
import pathlib
import re, os, shutil


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

def build_full_address(df, prefix=''):
    """Concatenate address pieces in different columns to get a full address,
    including city, state, and zipcode.
    """
    # Code copied from:
    # 2023_08_04_check_address_ids_2023_07_28_08_33_09.ipynb
    address_fields = ['street_number', 'street_name', 'unit_number', 'city', 'state', 'zipcode']
    address_colname = 'address'
    if prefix:
        address_fields = [f'{prefix}_{field}' for field in address_fields]
        address_colname = f'{prefix}_{address_colname}'

    def ensure_empty_string_in_cats(col):
        """Add empty string to categories if necessary so that I can replace
        NaN with '' before concatenation.
        """
        if col.dtype == 'category' and col.isna().any() and '' not in col.cat.categories:
            col = col.cat.add_categories('')
        return col

    address_cols = [ensure_empty_string_in_cats(df[field]) for field in address_fields]
    address = address_cols[0].fillna('').astype(str) # Need to ensure '' in categories to avoid an error here
    for col in address_cols[1:]:
        col = col.fillna('').astype(str) # Need to ensure '' in categories to avoid an error here
        # Only add a space in front of nonempty strings to avoid extra spaces between words when a field is missing
        col.loc[col != ''] = ' ' + col
        address += col
    address.rename(address_colname, inplace=True)
    return address

def remove_path(path):
    path = pathlib.Path(path)
    if path.is_file():
        os.remove(path)
    elif path.exists():
        shutil.rmtree(path)

def ensure_empty(path):
    remove_path(path)
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def dedupe_list(list_to_dedupe):
    """Removes duplicates from a list, preserving order."""
    # https://stackoverflow.com/a/6765391/
    seen = {}
    return [seen.setdefault(x, x) for x in list_to_dedupe if x not in seen]
