# Generate simulated input files for the PVS-like case study

This directory contains code to generate simulated input files for the larger case study.
For more details, see the notebook files themselves.

To run this, the steps are:
1. Create the appropriate conda environment (described in the README one directory up)
2. Run the data generation notebooks

## Run the pseudopeople dataset generation notebook

Run the notebook `generate_pseudopeople_simulated_datasets.ipynb`
in the case study environment.

Do **not** run this with JupyterLab or similar.
It is designed to contain no output, and be run with Papermill.
You can see example runs saved to `generate_pseudopeople_simulated_datasets_small_sample.ipynb`
and `generate_pseudopeople_simulated_datasets_usa.ipynb`.

## Run the data (reference file) generation notebook

Run the notebook `generate_simulated_data.ipynb`
in the case study environment.

Do **not** run this with JupyterLab or similar.
It is designed to contain no output, and be run with Papermill.
You can see example runs saved to `generate_simulated_data_small_sample.ipynb`
and `generate_simulated_data_usa.ipynb`.