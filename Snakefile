### Setup ###

import shutil, os, yaml

from person_linkage_case_study_utils.snakemake_utils import config_from_layers, get_directory_wrapper_if_necessary, dict_to_papermill

snakemake_config = config

with open('config/defaults.yaml') as stream:
    config_defaults = yaml.safe_load(stream)

if os.path.isfile('config/overrides.yaml'):
    with open('config/overrides.yaml') as stream:
        config_overrides = yaml.safe_load(stream)
else:
    config_overrides = {}

LAYERS = [
    ('defaults', config_defaults),
    ('user_file_overrides', config_overrides),
    ('snakemake_overrides', snakemake_config)
]

config = config_from_layers(LAYERS)

# NOTE: For now, determining this from whether or not this *Snakefile* is being
# run inside a conda environment and/or venv.

# NOTE: We do not use the built-in Snakemake-conda integration because
# it only supports environment *names*, not paths.
use_conda = "CONDA_PREFIX" in os.environ

if use_conda:
    conda_path = os.environ["CONDA_EXE"]
    conda_prefix = os.environ["CONDA_PREFIX"]
    conda_activate_part = f'''
    source {Path(conda_path).parent.parent / "etc/profile.d/conda.sh"}
    conda activate {conda_prefix}
    '''
else:
    conda_path = None
    conda_prefix = None
    conda_activate_part = ''

# https://stackoverflow.com/a/1883251/
use_venv = sys.prefix != sys.base_prefix

if use_venv:
    venv_path = sys.prefix
    venv_activate_part = f'source {venv_path}/bin/activate'
else:
    venv_path = None
    venv_activate_part = ''

if "singularity_bindings" in config:
    singularity_bindings = config["singularity_bindings"]
else:
    singularity_bindings = []

singularity_bindings.append('/tmp')

if use_conda:
    singularity_bindings.append(Path(conda_path).parent.parent)

if use_venv:
    singularity_bindings.append(venv_path)

if len(singularity_bindings) > 0:
    singularity_args = f'-B {",".join([str(s) for s in singularity_bindings])}'
    workflow._singularity_args += ' ' + singularity_args

executed_notebooks_dir = f'diagnostics/executed_notebooks/{config["data_to_use"]}'

# The first rule in the Snakefile is what will be generated by default.
# We have a dummy rule that depends on the accuracy notebook to ensure the
# entire workflow is run.
rule all:
    input:
        f'{executed_notebooks_dir}/04_calculate_ground_truth_accuracy.ipynb'

### Generate pseudopeople-simulated datasets ###

generate_pseudopeople_simulated_datasets_papermill_params = {
    'data_to_use': config["data_to_use"],
    'output_dir': f'{config["root_output_dir"]}/01_generate_pseudopeople_simulated_datasets/',
    'very_noisy': config["data_to_use"] == 'small_sample',
    **dict(config["papermill_params"][config["data_to_use"]]["generate_pseudopeople_simulated_datasets"])
}

output_wrapper = get_directory_wrapper_if_necessary(generate_pseudopeople_simulated_datasets_papermill_params)

# Programmatically generate the list of all datasets this will output,
# since there are many
pseudopeople_simulated_datasets = {
    'census': [2030],
    'ssa_numident': None,
    'taxes_1040': range(2025, 2030),
    'taxes_w2_and_1099': range(2025, 2030),
}

pseudopeople_simulated_datasets_paths = []

for dataset_name, years in pseudopeople_simulated_datasets.items():
    if years is None:
        pseudopeople_simulated_datasets_paths.append(f'simulated_{dataset_name}.parquet')
    else:
        pseudopeople_simulated_datasets_paths += [
            f'simulated_{dataset_name}_{year}.parquet'
            for year in years
        ]

pseudopeople_simulated_datasets_paths = [
    f'{config["root_output_dir"]}/01_generate_pseudopeople_simulated_datasets/{config["data_to_use"]}/{p}'
    for p in pseudopeople_simulated_datasets_paths
]

# NOTE: Including conda_activate_part and venv_activate_part isn't strictly necessary
# when rules inherit environment variables from the Python process running this Snakefile,
# but that won't be the case for Singularity rules or if running with a non-local Snakemake executor.
# We include them in all rules to be explicit.
rule generate_pseudopeople_simulated_datasets:
    input: "01_generate_pseudopeople_simulated_datasets.ipynb"
    log: f'{executed_notebooks_dir}/01_generate_pseudopeople_simulated_datasets.ipynb'
    output:
        [output_wrapper(p) for p in pseudopeople_simulated_datasets_paths]
    shell:
        f"""
        {conda_activate_part}
        {venv_activate_part}
        papermill {{input}} {{log}} {dict_to_papermill(generate_pseudopeople_simulated_datasets_papermill_params)} -k python3
        """

### Generate case study files ###

# "Case study files" here means the simulated CUF and reference files.
# These are processed versions of the pseudopeople outputs generated in
# the previous step.

generate_case_study_files_papermill_params = {
    'data_to_use': config["data_to_use"],
    'input_dir': f'{config["root_output_dir"]}/01_generate_pseudopeople_simulated_datasets/',
    'output_dir': f'{config["root_output_dir"]}/02_generate_case_study_files/',
    **dict(config["papermill_params"][config["data_to_use"]]["generate_case_study_files"]),
}

output_wrapper = get_directory_wrapper_if_necessary(generate_case_study_files_papermill_params)

case_study_files = [
    'census_numident', 'alternate_dob_numident', 'alternate_name_numident',
    'geobase_reference_file', 'name_dob_reference_file',
    'census_2030',
]

case_study_files_paths = []

for file in case_study_files:
    case_study_files_paths += [
        f'simulated_{file}.parquet',
        f'simulated_{file}_ground_truth.parquet',
    ]

# This one doesn't follow the _ground_truth pattern.
case_study_files_paths += ['simulated_pik_simulant_pairs.parquet']

case_study_files_paths = [f'{config["root_output_dir"]}/02_generate_case_study_files/{config["data_to_use"]}/{p}' for p in case_study_files_paths]

rule generate_case_study_files:
    input:
        ["02_generate_case_study_files.ipynb"] +
        pseudopeople_simulated_datasets_paths
    log: f'{executed_notebooks_dir}/02_generate_case_study_files.ipynb'
    output:
        [output_wrapper(p) for p in case_study_files_paths]
    shell:
        f"""
        {conda_activate_part}
        {venv_activate_part}
        papermill {{input[0]}} {{log}} {dict_to_papermill(generate_case_study_files_papermill_params)} -k python3
        """

### Link datasets ###

if config["papermill_params"][config["data_to_use"]]["link_datasets"]["splink_engine"] == "spark":
    # We need to run inside a container, which also means we need to use conda manually
    # instead of relying on the Snakemake-conda integration.
    if "custom_spark_container_path" in config:
        singularity_image = config["custom_spark_container_path"]
    else:
        singularity_image = "docker://apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63"
    
    singularity_path = shutil.which('singularity')
else:
    singularity_image = None
    singularity_path = None

link_datasets_papermill_params = {
    'data_to_use': config["data_to_use"],
    'input_dir': f'{config["root_output_dir"]}/02_generate_case_study_files/',
    'output_dir': f'{config["root_output_dir"]}/03_link_datasets/',
    'singularity_path': singularity_path,
    'singularity_image': singularity_image,
    'singularity_args': singularity_args,
    'conda_path': conda_path,
    'conda_prefix': conda_prefix,
    'venv_path': venv_path,
    **dict(config["papermill_params"][config["data_to_use"]]["link_datasets"]),
}

output_wrapper = get_directory_wrapper_if_necessary(link_datasets_papermill_params)

linkage_outputs = [
    f'{config["root_output_dir"]}/03_link_datasets/{config["data_to_use"]}/census_2030_piked.parquet',
    f'{config["root_output_dir"]}/03_link_datasets/{config["data_to_use"]}/confirmed_piks.parquet',
]

rule link_datasets:
    container: singularity_image
    benchmark:
        f'benchmarks/benchmark-{config["data_to_use"]}.txt'
    input:
        ["03_link_datasets.ipynb"] +
        [p for p in case_study_files_paths if 'ground_truth' not in p and 'pik_simulant_pairs' not in p]
    log: f'{executed_notebooks_dir}/03_link_datasets.ipynb'
    output:
        [output_wrapper(p) for p in linkage_outputs]
    shell:
        f"""
        {conda_activate_part}
        {venv_activate_part}
        papermill {{input[0]}} {{log}} {dict_to_papermill(link_datasets_papermill_params)} -k python3
        """

### Calculate ground-truth accuracy ###

calculate_ground_truth_accuracy_papermill_params = {
    'data_to_use': config["data_to_use"],
    'case_study_files_dir': f'{config["root_output_dir"]}/02_generate_case_study_files/',
    'case_study_output_dir': f'{config["root_output_dir"]}/03_link_datasets/',
    **dict(config["papermill_params"][config["data_to_use"]]["calculate_ground_truth_accuracy"]),
}

rule calculate_ground_truth_accuracy:
    input:
        ["04_calculate_ground_truth_accuracy.ipynb"] + case_study_files_paths + linkage_outputs
    log: f'{executed_notebooks_dir}/04_calculate_ground_truth_accuracy.ipynb'
    shell:
        f"""
        {conda_activate_part}
        {venv_activate_part}
        papermill {{input[0]}} {{log}} {dict_to_papermill(calculate_ground_truth_accuracy_papermill_params)} -k python3
        """


# For debugging, it may be helpful to run the notebooks through Snakemake, but in the browser.
# This can be achieved by replacing any shell command(s) with:
# f"""
# # Snakemake rules run with a non-default home directory, so you will want to set this to your home
# # to use your Jupyter settings
# export HOME='<your home>'
# papermill --prepare-only {{input[0]}} {{log}} {dict_to_papermill(papermill_params)} -k python3
# jupyter lab --browser ':' --no-browser --ServerApp.quit_button=True --ip 0.0.0.0 --port 28508 {{log}}
# """