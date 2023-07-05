# PVS-like case study

This directory contains a case study emulating the Census Bureau's Person Identification
Validation System.
For more details, see the notebook files themselves.

To run this, the steps are:
1. Create the appropriate conda environment
2. Run the data generation notebook
3. Run the linking notebook

## Create the appropriate conda environment

To more or less exactly replicate the conda environment that was used when making
this case study, run

```
$ conda env create --file=conda_environment.yaml
```

in this directory.

If for whatever reason that doesn't work/has conflicts, you can *approximately*
recreate the environment with:

```
$ conda create -n pvs_like_case_study python=3.10
$ conda activate pvs_like_case_study
$ pip install pandas numpy matplotlib pseudopeople splink jupyterlab jellyfish
```

## Run the data generation notebook

Run the notebook `generate_simulated_data/generate_simulated_data_small_sample.ipynb`
in the `pvs_like_case_study` environment created above.

Or, if you'd like to run it as a Python script:

```
$ ./convert_notebook.sh generate_simulated_data/generate_simulated_data_small_sample # only necessary if you've edited the notebook
$ python generate_simulated_data/generate_simulated_data_small_sample.py
```

## Run the linking notebook

Run the notebook `pvs_like_case_study_sample_data.ipynb`
in the `pvs_like_case_study` environment created above.

Or, if you'd like to run it as a Python script:

```
$ ./convert_notebook.sh pvs_like_case_study_sample_data # only necessary if you've edited the notebook
$ python pvs_like_case_study_sample_data.py
```