import pathlib
import os, shutil


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
