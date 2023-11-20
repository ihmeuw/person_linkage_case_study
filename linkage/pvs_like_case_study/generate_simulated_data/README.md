# Generate simulated input files for the PVS-like case study

This directory contains code to generate simulated input files for the larger case study.
For more details, see the notebook files themselves.

To run this, the steps are:
1. Create the appropriate conda environment
2. Run the data generation notebooks

## Create the appropriate conda environment

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
$ pip freeze | grep -v 'file:///' | grep -v ' \-e' > conda_environment_lock_pip.txt
```

## Run the pseudopeople dataset generation notebook

Run the notebook `generate_pseudopeople_simulated_datasets.ipynb`
in the `generate_simulated_data` environment created above.

Do **not** run this with JupyterLab or similar.
It is designed to contain no output, and be run with Papermill.
You can see example runs saved to `generate_pseudopeople_simulated_datasets_small_sample.ipynb`
and `generate_pseudopeople_simulated_datasets_full_usa.ipynb`.

## Run the data (reference file) generation notebook

Run the notebook `generate_simulated_data_small_sample.ipynb`
in the `generate_simulated_data` environment created above.

This notebook is also designed to be run with Papermill *when not using default settings*.
For the default settings (small sample with Pandas), the outputs are saved directly in the notebook.