# Contributing

## Linting

Install the project with `pip install -e .[dev]`
then run `black .` and `isort`.

## Updating dependencies

This project has pinned lockfiles and abstract dependencies.
They need to be updated in concert.

The abstract dependencies are stored in `setup.py`.
There are several extras depending on what the user
wants to install (Dask and/or Spark).

The pin files are `conda.lock.txt` and the `pip.lock-*.txt` files.

### Updating pip dependencies

These dependencies must be updated all at once.

1. Create a new conda environment from the existing conda lock file.
2. `pip install -e .`
3. `pip freeze | grep -v 'file:///' | grep -v '\-e ' > pip.lock.txt`
4. `pip install -e .[dask]`
5. `comm -13 <(cat pip.lock.txt | sort) <(pip freeze | grep -v 'file:///' | grep -v '\-e ' | sort) > pip.lock-dask.txt`
6. `pip install -e .[spark]`
7. `comm -13 <(cat pip.lock.txt pip.lock-dask.txt | sort) <(pip freeze | grep -v 'file:///' | grep -v '\-e ' | sort) > pip.lock-spark.txt`
8. `pip install -e .[dev]`
9. `comm -13 <(cat pip.lock.txt pip.lock-dask.txt pip.lock-spark.txt | sort) <(pip freeze | grep -v 'file:///' | grep -v '\-e ' | sort) > pip.lock-dev.txt`

### Updating conda dependencies

Currently Python is our only conda dependency, so this should be rare.

1. Create a new conda environment from the existing conda lock file.
2. `conda install <packages to add/update> --no-update-deps`
3. `conda list --explicit > conda.lock.txt`.
4. Proceed with installing from the pip lock files.