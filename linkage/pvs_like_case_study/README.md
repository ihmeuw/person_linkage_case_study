# PVS-like case study

This directory contains a case study emulating the Census Bureau's Person Identification
Validation System.
For more details, see the notebook files themselves.

To run this, the steps are:
1. Create the appropriate conda environment
2. Run the linking notebook

Note that this assumes you have already generated the input files; if you haven't
done this, look at the `generate_simulated_data` subdirectory and follow the directions
in its README before continuing.

## Create the appropriate conda environment

To essentially exactly replicate the conda environment that was used when making
this case study, run

```
$ conda create --name pvs_like_case_study --file conda_environment_lock_conda.txt
$ conda activate pvs_like_case_study
$ pip install -r conda_environment_lock_pip.txt
```

in this directory.
In rare cases that may not work due to a pulled package version.

You can *approximately* recreate the environment (e.g. if you want to update
all dependencies) with:

```
$ conda env create -n pvs_like_case_study --file conda_environment.yaml
```

If you do this, you can re-generate the lock files like so:

```
$ conda activate pvs_like_case_study
$ conda list --explicit > conda_environment_lock_conda.txt
# Greps:
# - exclude python packages installed via conda, which are linked by file
# - exclude editable packages
$ pip freeze | grep -v 'file:///' | grep -v '\-e' > conda_environment_lock_pip.txt
```

## Run the linking notebook

Run the notebook `pvs_like_case_study_sample_data.ipynb`
in the `pvs_like_case_study` environment created above.

Or, if you'd like to run it as a Python script:

```
$ ./convert_notebook.sh pvs_like_case_study_sample_data # only necessary if you've edited the notebook
$ python pvs_like_case_study_sample_data.py
```