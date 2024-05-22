import shutil, os

configfile: "snakemake_config_defaults.yaml"

if os.path.isfile("snakemake_config_overrides.yaml"):
    configfile: "snakemake_config_overrides.yaml"

# Need the full conda path for singularity (must be bound into container!)
import shutil
conda_path = shutil.which('conda')

rule all:
    input:
        f'ground_truth_accuracy_{config["data_to_use"]}{config["custom_run_suffix"]}.ipynb'

generate_pseudopeople_simulated_datasets_papermill_params = {
    'data_to_use': config["data_to_use"],
    'output_dir': f'{config["root_output_dir"]}/generate_simulated_data/',
    'very_noisy': config["data_to_use"] == 'small_sample',
    **config['papermill_params'][config["data_to_use"]]['generate_pseudopeople_simulated_datasets']
}

if generate_pseudopeople_simulated_datasets_papermill_params["compute_engine"] == 'pandas':
    output_wrapper = lambda x: x
else:
    output_wrapper = lambda x: directory(x)

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
    f'{config["root_output_dir"]}/generate_simulated_data/{config["data_to_use"]}/pseudopeople_simulated_datasets/{p}'
    for p in pseudopeople_simulated_datasets_paths
]

def dict_to_papermill(d):
    return ' '.join([f'-p {k} {v}' for k, v in d.items()])

rule generate_pseudopeople_simulated_datasets:
    input:
        "generate_simulated_data/generate_pseudopeople_simulated_datasets.ipynb"
    log: f'generate_simulated_data/generate_pseudopeople_simulated_datasets_{config["data_to_use"]}{config["custom_run_suffix"]}.ipynb'
    output:
        [output_wrapper(p) for p in pseudopeople_simulated_datasets_paths]
    conda: config["conda_environment_name"]
    shell:
        f"papermill {{input}} {{log}} {dict_to_papermill(generate_pseudopeople_simulated_datasets_papermill_params)} -k python3"

generate_case_study_files_papermill_params = {
    'data_to_use': config["data_to_use"],
    'output_dir': f'{config["root_output_dir"]}/generate_simulated_data/',
    **config['papermill_params'][config["data_to_use"]]['generate_case_study_files']
}

if generate_case_study_files_papermill_params["compute_engine"] == 'pandas':
    output_wrapper = lambda x: x
else:
    output_wrapper = lambda x: directory(x)

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

case_study_files_paths = [f'{config["root_output_dir"]}/generate_simulated_data/{config["data_to_use"]}/{p}' for p in case_study_files_paths]

rule generate_case_study_files:
    input:
        ["generate_simulated_data/generate_simulated_data.ipynb"] +
        pseudopeople_simulated_datasets_paths
    log: f'generate_simulated_data/generate_simulated_data_{config["data_to_use"]}{config["custom_run_suffix"]}.ipynb'
    output:
        [output_wrapper(p) for p in case_study_files_paths] +
        [output_wrapper(f'{config["root_output_dir"]}/generate_simulated_data/{config["data_to_use"]}/simulated_pik_simulant_pairs.parquet')]
    conda: config["conda_environment_name"]
    shell:
        f"papermill {{input[0]}} {{log}} {dict_to_papermill(generate_case_study_files_papermill_params)} -k python3"

link_datasets_papermill_params = {
    'data_to_use': config["data_to_use"],
    'input_dir': f'{config["root_output_dir"]}/generate_simulated_data/',
    'output_dir': f'{config["root_output_dir"]}/results/',
    **config['papermill_params'][config["data_to_use"]]['link_datasets']
}

if link_datasets_papermill_params["compute_engine"] == 'pandas':
    output_wrapper = lambda x: x
else:
    output_wrapper = lambda x: directory(x)

linkage_outputs = [
    f'{config["root_output_dir"]}/results/{config["data_to_use"]}/census_2030_piked.parquet',
    f'{config["root_output_dir"]}/results/{config["data_to_use"]}/confirmed_piks.parquet',
]

rule link_datasets:
    container: "./spark.sif"
    benchmark:
        f'benchmarks/benchmark-{config["data_to_use"]}.txt'
    input:
        ["person_linkage_case_study.ipynb"] +
        [p for p in case_study_files_paths if 'ground_truth' not in p and 'pik_simulant_pairs' not in p]
    log: f'person_linkage_case_study_{config["data_to_use"]}{config["custom_run_suffix"]}.ipynb'
    output:
        [output_wrapper(p) for p in linkage_outputs]
    shell:
        f'{conda_path} run --no-capture-output -n {config["conda_environment_name"]} papermill {{input[0]}} {{log}} {dict_to_papermill(link_datasets_papermill_params)} -k python3'

calculate_ground_truth_papermill_accuracy_params = {
    'data_to_use': config["data_to_use"],
    'simulated_data_output_dir': f'{config["root_output_dir"]}/generate_simulated_data/',
    'case_study_output_dir': f'{config["root_output_dir"]}/results/',
    **config['papermill_params'][config["data_to_use"]]['calculate_ground_truth_accuracy']
}

rule calculate_ground_truth_accuracy:
    input:
        ["ground_truth_accuracy.ipynb"] + case_study_files_paths + linkage_outputs
    log: f'ground_truth_accuracy_{config["data_to_use"]}{config["custom_run_suffix"]}.ipynb'
    conda: config["conda_environment_name"]
    shell:
        f"papermill {{input[0]}} {{log}} {dict_to_papermill(calculate_ground_truth_papermill_accuracy_params)} -k python3"
