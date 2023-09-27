# Generate simulated input files for the PVS-like case study

This directory contains code to generate simulated input files for the larger case study.
For more details, see the notebook files themselves.

To run this, the steps are:
1. Create the appropriate conda environment
2. Run the data generation notebook

## Create the appropriate conda environment

### Non-distributed pandas

To essentially exactly replicate the conda environment that was used when making
this case study, run

```
$ conda create --name generate_simulated_data --file conda_environment_lock_conda.txt
$ conda activate generate_simulated_data
$ pip install -r conda_environment_lock_pip.txt
```

in this directory.
In rare cases that may not work due to a pulled package version.

You can *approximately* recreate the environment (e.g. if you want to update
all dependencies) with:

```
$ conda env create -n generate_simulated_data --file conda_environment.yaml
```

If you do this, you can re-generate the lock files like so:

```
$ conda activate generate_simulated_data
$ conda list --explicit > conda_environment_lock_conda.txt
# Greps:
# - exclude python packages installed via conda, which are linked by file
# - exclude editable packages
$ pip freeze | grep -v 'file:///' | grep -v '\-e' > conda_environment_lock_pip.txt
```

### Distributed with Modin

This follows the same process, but has a few extra dependencies.
Everywhere in the above instructions where `conda_environment` is used in a file name,
replace it with `modin_conda_environment`.

## Run the data generation notebook

Run the notebook `generate_simulated_data_small_sample.ipynb`
in the `generate_simulated_data` environment created above.

Or, if you'd like to run it as a Python script:

```
$ ../convert_notebook.sh generate_simulated_data_small_sample # only necessary if you've edited the notebook
$ python generate_simulated_data_small_sample.py
```