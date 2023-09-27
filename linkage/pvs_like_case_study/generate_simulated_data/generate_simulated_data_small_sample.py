#!/usr/bin/env python3
# coding: utf-8

# This script was automatically generated from a Jupyter notebook.
# Edit with care -- substantive edits should go in the notebook,
# or they will be overwritten the next time this script is generated.

# DO NOT EDIT if this notebook is not called generate_simulated_data_small_sample.ipynb!
# This notebook is designed to be run with papermill; this cell is tagged 'parameters'
# If running with the default parameters, you can overwrite this notebook; otherwise,
# save it to another filename.
# TODO: Rename the notebook to omit 'small_sample' in the filename and omit all outputs
# from the 'canonical version'
data_to_use = 'small_sample'
output_dir = 'output'
compute_engine = 'pandas'
num_jobs = 3
memory_per_job = "50GB"
    
# ! date
    
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
    
import pseudopeople as psp
import numpy as np
import os
import logging
    
psp.__version__
    
# Importing pandas for access, regardless of whether we are using it as the compute engine
import pandas
    
if compute_engine == 'pandas':
    import pandas as pd
elif compute_engine.startswith('modin'):
    if compute_engine.startswith('modin_dask_'):
        import modin.config as modin_cfg
        modin_cfg.Engine.put("dask") # Use dask instead of ray (which is the default)

        import dask

        if compute_engine == 'modin_dask_distributed':
            from dask_jobqueue import SLURMCluster

            num_processes_per_job = 1
            cluster = SLURMCluster(
                queue='long.q',
                account="proj_simscience",
                # If you give dask workers more than one core, they will use it to
                # run more tasks at once.
                # There doesn't appear to be an easy way to have more than one thread
                # per worker but use them all for multi-threading code in one task
                # at a time (note that processes=1 does *not* do this).
                # It can be done with custom resource definitions, but that seems like
                # much more trouble than it's worth, since it would require modifying
                # both pseudopeople and Modin to use them.
                cores=1,
                memory=memory_per_job,
                walltime="10-00:00:00",
                # Dask distributed looks at OS-reported memory to decide whether a worker is running out.
                # If the memory allocator is not returning the memory to the OS promptly (even when holding onto it
                # is smart), it will lead Dask to make bad decisions.
                # By default, pyarrow uses jemalloc, but I could not get that to release memory quickly.
                # Even this doesn't seem to be completely working, but in combination with small-ish partitions
                # it seems to do okay -- unmanaged memory does seem to shrink from time to time, which it wasn't
                # previously doing.
                job_script_prologue="export ARROW_DEFAULT_MEMORY_POOL=system\nexport MALLOC_TRIM_THRESHOLD_=0",
            )

            cluster.scale(n=num_jobs)
            # Supposedly, this will start new jobs if the existing
            # ones fail for some reason.
            # https://stackoverflow.com/a/61295019
            cluster.adapt(minimum_jobs=num_jobs, maximum_jobs=num_jobs)

            from distributed import Client
            client = Client(cluster)
        else:
            from distributed import Client
            cpus_available = int(os.environ['SLURM_CPUS_ON_NODE'])
            client = Client(n_workers=int(cpus_available / 2), threads_per_worker=2)

        # Why is this necessary?!
        # For some reason, if I don't set NPartitions, it seems to default to 0?!
        num_row_groups = 334
        modin_cfg.NPartitions.put(min(num_jobs * num_processes_per_job * 3, num_row_groups))

        display(client)
    elif compute_engine == 'modin_ray':
        # Haven't worked on distributing this across multiple nodes
        import ray
        ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}}, num_cpus=int(os.environ['SLURM_CPUS_ON_NODE']))
    else:
        # Use serial Python backend (good for debugging errors)
        import modin.config as modin_cfg
        modin_cfg.IsDebug.put(True)

    import modin.pandas as pd

    # https://modin.readthedocs.io/en/stable/usage_guide/advanced_usage/progress_bar.html
    from modin.config import ProgressBar
    ProgressBar.enable()
    
assert data_to_use in ('small_sample', 'usa')
pseudopeople_input_dir = None if data_to_use == 'small_sample' else '/mnt/team/simulation_science/priv/engineering/vivarium_census_prl_synth_pop/results/release_02_yellow/full_data/united_states_of_america/2023_07_28_08_33_09/final_results/2023_08_16_09_58_54/pseudopeople_input_data_usa/'
    
default_configuration = psp.get_config()
    
# Helper functions for changing the default configuration according to a pattern
def column_noise_value(dataset, column, noise_type, default_value):
    if dataset in ('decennial_census', 'taxes_w2_and_1099', 'social_security'):
        if noise_type == "make_typos":
            if column == "middle_initial":
                # 5% of middle initials (which are all a single token anyway) are wrong.
                return {"cell_probability": 0.05, "token_probability": 1}
            elif column in ("first_name", "last_name", "street_name"):
                # 10% of these text columns were entered carelessly, at a rate of 1 error
                # per 10 characters.
                # The pseudopeople default is 1% careless.
                return {"cell_probability": 0.1, "token_probability": 0.1}
        elif noise_type == "write_wrong_digits":
            # 10% of number columns were written carelessly, at a rate of 1 error
            # per 10 characters.
            # The pseudopeople default is 1% careless.
            # Note that this is applied on top of (the default lower levels of) typos,
            # since typos also apply to numeric characters.
            return {"cell_probability": 0.1, "token_probability": 0.1}

    return default_value

def row_noise_value(dataset, noise_type, default_value):
    return default_value
    
custom_configuration = {
    dataset: {
        noise_category: (
            ({
                column: {
                    noise_type: column_noise_value(dataset, column, noise_type, noise_type_config)
                    for noise_type, noise_type_config in column_config.items()
                }
                for column, column_config in noise_category_config.items()
            }
            if noise_category == "column_noise" else
            {
                noise_type: row_noise_value(dataset, noise_type, noise_type_config)
                for noise_type, noise_type_config in noise_category_config.items()
            })
        )
        for noise_category, noise_category_config in dataset_config.items()
    }
    for dataset, dataset_config in default_configuration.items()
}
    
def add_unique_record_id(df, dataset_name):
    df = df.reset_index().rename(columns={'index': 'record_id'})
    df['record_id'] = f'{dataset_name}_' + df.record_id.astype(str)
    return df

# Initializes a table listing the pairs between record_ids and source record_ids.
# Should only be called on "source records"; that is, records that
# come directly out of pseudopeople.
def record_id_to_single_source_record_pairs(df, source_col='record_id'):
    if source_col == 'record_id':
        # We can't have duplicate column names, so we make a new column
        # literally called 'source_col'
        df = df.assign(source_col=lambda df: df[source_col])
        source_col = 'source_col'

    return df[['record_id', source_col]].rename(columns={source_col: 'source_record_id'})
    
# Operations that aggregate records, combining the source_record_ids column
# between all records that are aggregated into a single row

def merge_preserving_source_records(dfs, source_record_pairings, new_record_id_prefix, *args, **kwargs):
    assert len(dfs) == len(source_record_pairings)
    for df in dfs:
        assert 'record_id' in df.columns

    result = dfs[0]
    source_record_pairs = source_record_pairings[0]
    dfs_and_source_record_pairs_to_combine = list(zip(dfs[1:], source_record_pairings[1:]))
    for index, (df_to_merge, source_record_pairs_to_merge) in enumerate(dfs_and_source_record_pairs_to_combine):
        result = (
            result.merge(df_to_merge, *args, **kwargs)
        )
        if index == len(dfs_and_source_record_pairs_to_combine) - 1:
            # Since this is the last step, these are the record_ids that will actually be returned
            accumulate_step_record_id_prefix = new_record_id_prefix
        else:
            # A dummy intermediate -- this shouldn't be exposed to the user
            accumulate_step_record_id_prefix = f'merge_iter_{index}'

        result = add_unique_record_id(result, accumulate_step_record_id_prefix)
        source_record_pairs = pd.concat([
            # The pairs that were already in result
            source_record_pairs
                .rename(columns={'record_id': 'record_id_x'})
                .merge(result[['record_id', 'record_id_x']], on='record_id_x')
                .drop(columns=['record_id_x']),
            # The new ones
            source_record_pairs_to_merge
                .rename(columns={'record_id': 'record_id_y'})
                .merge(result[['record_id', 'record_id_y']], on='record_id_y')
                .drop(columns=['record_id_y']),
        ])
        result = result.drop(columns=['record_id_x', 'record_id_y'])

    return result, source_record_pairs

def dedupe_preserving_source_records(df, source_record_pairs, columns_to_dedupe, new_record_id_prefix):#, source_records_col='source_record_ids'):
    result = df[columns_to_dedupe].drop_duplicates()
    result = add_unique_record_id(result, new_record_id_prefix)
    df_to_result_mapping = (
        df[['record_id'] + columns_to_dedupe]
            .rename(columns={'record_id': 'record_id_pre_dedupe'})
            .merge(result, on=columns_to_dedupe)
            [['record_id', 'record_id_pre_dedupe']]
    )
    result_source_record_pairs = (
        source_record_pairs
            .rename(columns={'record_id': 'record_id_pre_dedupe'})
            .merge(df_to_result_mapping, on='record_id_pre_dedupe')
            .drop(columns=['record_id_pre_dedupe'])
    )
    return result, result_source_record_pairs

def concat_preserving_source_records(dfs, source_record_pairings, new_record_id_prefix):
    dfs = [df.rename(columns={'record_id': 'record_id_pre_concat'}) for df in dfs]
    result = pd.concat(dfs, ignore_index=True)
    result = add_unique_record_id(result, new_record_id_prefix)

    record_id_mapping = (
        result[['record_id', 'record_id_pre_concat']]
    )
    result_source_record_pairs = (
        pd.concat(source_record_pairings, ignore_index=False)
            .rename(columns={'record_id': 'record_id_pre_concat'})
            .merge(record_id_mapping, on='record_id_pre_concat', validate='m:1')
            .drop(columns=['record_id_pre_concat'])
    )

    return result.drop(columns=['record_id_pre_concat']), result_source_record_pairs
    
psp_kwargs = {
    'config': custom_configuration,
    'source': pseudopeople_input_dir,
}
if 'modin' in compute_engine:
    engine_kwargs['engine'] = 'modin'
    
# %%time

ssa_numident = psp.generate_social_security(
    year=2029,
    **psp_kwargs,
)
ssa_numident = add_unique_record_id(ssa_numident, 'ssa_numident')
ssa_numident_source_record_pairs = record_id_to_single_source_record_pairs(ssa_numident)
ssa_numident
    
ssa_numident_ground_truth = ssa_numident.set_index('record_id').simulant_id
ssa_numident = ssa_numident.drop(columns=['simulant_id'])
    
tax_years = list(range(2025, 2030))
tax_years
    
# %%time

# Combine 1040 for all years, adding a tax_year column to track which tax year each row came from.
taxes_1040 = pd.concat([
    psp.generate_taxes_1040(
        year=year,
        **psp_kwargs,
    ).assign(tax_year=year, tax_form='1040') for year in tax_years
], ignore_index=True)
taxes_1040 = add_unique_record_id(taxes_1040, '1040')
taxes_1040_source_record_pairs = record_id_to_single_source_record_pairs(taxes_1040)
taxes_1040
    
taxes_1040_ground_truth = taxes_1040.set_index('record_id').simulant_id
taxes_1040 = taxes_1040.drop(columns=['simulant_id'])
    
# %%time

# Combine W2/1099 for all years, adding a tax_year column to track which tax year each row came from.
w2_1099 = pd.concat([
    psp.generate_taxes_w2_and_1099(
        year=year,
        **psp_kwargs,
    ).assign(tax_year=year) for year in tax_years
], ignore_index=True)
w2_1099 = add_unique_record_id(w2_1099, 'w2_1099')
w2_1099_source_record_pairs = record_id_to_single_source_record_pairs(w2_1099)
w2_1099
    
w2_1099_ground_truth = w2_1099.set_index('record_id').simulant_id
w2_1099 = w2_1099.drop(columns=['simulant_id'])
    
taxes, taxes_source_record_pairs = concat_preserving_source_records(
    [taxes_1040, w2_1099],
    [taxes_1040_source_record_pairs, w2_1099_source_record_pairs],
    new_record_id_prefix='taxes',
)
    
taxes_ground_truth = pd.concat([taxes_1040_ground_truth, w2_1099_ground_truth])
    
# "... many of the [IRS] records contain only the first four letters of the last name."
# (Brown et al. 2023, p.30, footnote 19)
# This should be updated in pseudopeople but for now we do it here.
# Note that this truncation only matters for ITIN PIKing since for SSNs that are present in SSA we use name from SSA.
PROPORTION_OF_IRS_RECORDS_WITH_TRUNCATION = 0.4 # is this a good guess at "many" in the quote above?
idx_to_truncate = taxes.sample(frac=PROPORTION_OF_IRS_RECORDS_WITH_TRUNCATION, random_state=1234).index
taxes.loc[idx_to_truncate, 'last_name'] = taxes.loc[idx_to_truncate, 'last_name'].str[:4]
taxes.loc[idx_to_truncate, 'last_name']
    
# %%time

census_2030 = psp.generate_decennial_census(
    year=2030,
    **psp_kwargs,
)
census_2030 = add_unique_record_id(census_2030, 'census_2030')
census_2030_source_record_pairs = record_id_to_single_source_record_pairs(census_2030)
census_2030
    
census_2030_ground_truth = census_2030.set_index('record_id').simulant_id
census_2030 = census_2030.drop(columns=['simulant_id'])
    
source_record_ground_truth = pd.concat([
    ssa_numident_ground_truth,
    taxes_ground_truth,
    census_2030_ground_truth,
])
source_record_ground_truth
    
def fill_dates(df, fill_with):
    return (
        # Replace invalid dates with nans
        pd.to_datetime(df.event_date, format='%Y%m%d', errors='coerce')
            .fillna(pd.to_datetime('2100-01-01' if fill_with == 'latest' else '1900-01-01'))
    )

def best_data_from_columns(df, columns, best_is_latest=True):
    # We don't want to throw out events with a missing/invalid date, so we'll fill them with the value *least* likely to be chosen
    # (earlier than all values if taking the latest, later than all values if taking the earliest).
    fill_with = 'earliest' if best_is_latest else 'latest'

    result = (
        df
            # Without mutating the existing date column, get one that is actually
            # a date type and can be used for sorting.
            # Note: we actually convert this to an integer for sorting purposes, because Modin was having trouble
            # sorting by it as an actual datetime
            .assign(event_date_for_sort=lambda df: fill_dates(df, fill_with=fill_with).astype(np.int64) // 10 ** 9)
            .sort_values('event_date_for_sort')
            .dropna(subset=columns, how='all')
            .drop_duplicates('ssn', keep=('last' if best_is_latest else 'first'))
            [['record_id', 'ssn'] + columns]
    )
    return result, record_id_to_single_source_record_pairs(result)

best_name, best_name_source_record_pairs = best_data_from_columns(
    ssa_numident,
    columns=['first_name', 'middle_name', 'last_name'],
)

best_date_of_birth, best_date_of_birth_source_record_pairs = best_data_from_columns(
    ssa_numident,
    columns=['date_of_birth'],
)

best_date_of_death, best_date_of_death_source_record_pairs = best_data_from_columns(
    ssa_numident[ssa_numident.event_type == 'death'],
    columns=['event_date'],
)
best_date_of_death = best_date_of_death.rename(columns={'event_date': 'date_of_death'})

census_numident, census_numident_source_record_pairs = merge_preserving_source_records(
    [best_name, best_date_of_birth, best_date_of_death],
    [best_name_source_record_pairs, best_date_of_birth_source_record_pairs, best_date_of_death_source_record_pairs],
    new_record_id_prefix='census_numident',
    on='ssn',
    how='left',
)
census_numident
    
alternate_name_numident, alternate_name_numident_source_record_pairs = dedupe_preserving_source_records(
    ssa_numident,
    ssa_numident_source_record_pairs,
    columns_to_dedupe=['ssn', 'first_name', 'middle_name', 'last_name'],
    new_record_id_prefix='alternate_name_numident',
)
alternate_name_numident
    
alternate_name_numident.groupby('ssn').size().describe()
    
alternate_name_numident[alternate_name_numident.ssn.duplicated(keep=False)].sort_values('ssn')
    
alternate_dob_numident, alternate_dob_numident_source_record_pairs = dedupe_preserving_source_records(
    ssa_numident,
    ssa_numident_source_record_pairs,
    columns_to_dedupe=['ssn', 'date_of_birth'],
    new_record_id_prefix='alternate_dob_numident',
)
alternate_dob_numident
    
alternate_dob_numident.groupby('ssn').size().describe()
    
alternate_dob_numident[alternate_dob_numident.ssn.duplicated(keep=False)].sort_values('ssn')
    
name_dob_numident_records, name_dob_numident_records_source_record_pairs = merge_preserving_source_records(
    [alternate_name_numident, alternate_dob_numident],
    [alternate_name_numident_source_record_pairs, alternate_dob_numident_source_record_pairs],
    on='ssn',
    how='left',
    new_record_id_prefix='name_dob_numident_records',
)
name_dob_numident_records
    
name_dob_numident_records[name_dob_numident_records.ssn.duplicated(keep=False)].sort_values('ssn')
    
# Analogous to the process of getting alternate names and dates of birth
# from SSA, we retain all versions of the name from taxes.
taxes_1040_with_itins = taxes_1040[taxes_1040.ssn.notnull() & taxes_1040.ssn.str.startswith('9')]
taxes_1040_with_itins_source_record_pairs = taxes_1040_source_record_pairs[taxes_1040_source_record_pairs.record_id.isin(taxes_1040_with_itins.record_id)]
name_for_itins, name_for_itins_source_record_pairs = dedupe_preserving_source_records(
    taxes_1040_with_itins,
    taxes_1040_with_itins_source_record_pairs,
    columns_to_dedupe=['ssn', 'first_name', 'middle_initial', 'last_name'],
    new_record_id_prefix='name_for_itins',
)
name_for_itins = name_for_itins.rename(columns={'middle_initial': 'middle_name'})
name_for_itins
    
name_for_itins.groupby('ssn').size().describe()
    
# Make sure these are disjoint sets of SSN values -- this could only happen
# if an SSN value in the Numident were corrupted into the ITIN range
assert set(name_dob_numident_records.ssn) & set(name_for_itins.ssn) == set()
    
name_dob_reference_file, name_dob_reference_file_source_record_pairs = concat_preserving_source_records(
    [name_dob_numident_records, name_for_itins],
    [name_dob_numident_records_source_record_pairs, name_for_itins_source_record_pairs],
    new_record_id_prefix='name_dob_reference_file',
)

name_dob_reference_file
    
address_cols = list(taxes.filter(like='mailing_address').columns)

def standardize_address_part(column):
    return (
        column
            # Remove leading or trailing whitespace
            .str.strip()
            # Turn any strings of consecutive whitespace into a single space
            .str.split().str.join(' ')
            # Normalize case
            .str.upper()
            # Normalize the word street as described in the example quoted above
            # In reality, there would be many rules like this
            .str.replace('\b(STREET|STR)\b', 'ST', regex=True)
            # Make sure missingness is represented consistently
            .replace('', np.nan)
    )

tax_addresses = (
    taxes.set_index(['record_id', 'ssn'])
        [address_cols]
        .apply(standardize_address_part)
        .reset_index()
)
addresses_by_ssn, addresses_by_ssn_source_record_pairs = dedupe_preserving_source_records(
    tax_addresses,
    taxes_source_record_pairs,
    columns_to_dedupe=['ssn'] + address_cols,
    new_record_id_prefix='addresses_by_ssn',
)
addresses_by_ssn
    
num_addresses = addresses_by_ssn.groupby('ssn').size().sort_values()
num_addresses
    
# Show some SSNs with a lot of address variation
addresses_by_ssn[addresses_by_ssn.ssn.isin(num_addresses.tail(10).index)].sort_values('ssn')
    
# Rough estimate of how many rows we should have in our reference file, once we do this Cartesian product
(
    len(name_dob_reference_file) *
    addresses_by_ssn.groupby('ssn').size().mean()
)
    
geobase_reference_file, geobase_reference_file_source_record_pairs = merge_preserving_source_records(
    [name_dob_reference_file, addresses_by_ssn],
    [name_dob_reference_file_source_record_pairs, addresses_by_ssn_source_record_pairs],
    on='ssn',
    how='left',
    new_record_id_prefix='geobase_reference_file',
)
geobase_reference_file
    
# TODO: This is really slow compared to pandas' mode.
# Mode should be added into Modin as a first-class citizen.
def mode(s):
    val_counts = s.value_counts()
    return val_counts.loc[val_counts == val_counts.max()].reset_index().loc[:, val_counts.index.name].rename(None)

def get_simulants_for_record_ids(record_ids, ground_truth=source_record_ground_truth):
    return tuple(ground_truth.loc[record_id] for record_id in record_ids)

def get_simulants_of_source_records(source_record_pairs, filter_record_ids=None, ground_truth=source_record_ground_truth):
    if filter_record_ids is not None:
        source_record_pairs = source_record_pairs.pipe(filter_record_ids)
    grouped_simulant_id = lambda: (
        source_record_pairs
            .merge(ground_truth.reset_index().rename(columns={'record_id': 'source_record_id'}), on='source_record_id')
            .groupby('record_id')
            .simulant_id
    )
    
    return (
        # NOTE: You'd expect to be able to just call .agg([mode, 'nunique']) instead of joining.
        # This is probably slower than that would be, but that doesn't work due to a Modin bug:
        # https://github.com/modin-project/modin/issues/6600
        pd.DataFrame(grouped_simulant_id().agg(mode)).merge(pd.DataFrame(grouped_simulant_id().agg('nunique').rename('nunique')), left_index=True, right_index=True)
    )
    
census_numident_ground_truth = get_simulants_of_source_records(census_numident_source_record_pairs)
census_numident_ground_truth
    
census_numident_ground_truth['nunique'].describe()
    
# We take the most common ground truth value.
alternate_name_numident_ground_truth = get_simulants_of_source_records(alternate_name_numident_source_record_pairs)
alternate_name_numident_ground_truth
    
# Again, as shown above, there are no SSN collisions.
alternate_name_numident_ground_truth['nunique'].describe()
    
alternate_dob_numident_ground_truth = get_simulants_of_source_records(alternate_dob_numident_source_record_pairs)
alternate_dob_numident_ground_truth
    
alternate_dob_numident_ground_truth['nunique'].describe()
    
name_dob_reference_file_ground_truth = get_simulants_of_source_records(name_dob_reference_file_source_record_pairs)
name_dob_reference_file_ground_truth
    
name_dob_reference_file_ground_truth['nunique'].describe()
    
geobase_reference_file_ground_truth_naive = get_simulants_of_source_records(geobase_reference_file_source_record_pairs)
geobase_reference_file_ground_truth_naive
    
# Now there are some collisions, due to "borrowed SSN" noise
geobase_reference_file_ground_truth_naive['nunique'].describe()
    
# The most collisions on one SSN
most_collisions_record_id = geobase_reference_file_ground_truth_naive.reset_index().sort_values('nunique', ascending=False).iloc[0].record_id
most_collisions_source_record_ids = geobase_reference_file_source_record_pairs[geobase_reference_file_source_record_pairs.record_id == most_collisions_record_id].source_record_id
most_collisions_tax_filings = taxes[taxes.merge(taxes_source_record_pairs, on='record_id').source_record_id.isin(most_collisions_source_record_ids)]
most_collisions_tax_filings
    
# Correct value: who actually has the SSN
ssa_numident[ssa_numident.ssn.isin(most_collisions_tax_filings.ssn.unique())].merge(ssa_numident_ground_truth, on='record_id')
    
# Let's prioritize the use of SSA records
geobase_reference_file_ground_truth_ssa_only = get_simulants_of_source_records(
    geobase_reference_file_source_record_pairs,
    filter_record_ids=lambda df: df[df.source_record_id.str.startswith('ssa_numident_')],
)
geobase_reference_file_ground_truth_ssa_only
    
geobase_reference_file_ground_truth_ssa_only['nunique'].describe()
    
geobase_reference_file_ground_truth = (
    geobase_reference_file_ground_truth_naive.join(geobase_reference_file_ground_truth_ssa_only, lsuffix='_naive', rsuffix='_ssa_only', how='outer')
        .assign(
            # When there are no SSA records (ITIN-based), use the standard mode
            simulant_id=lambda df: df.simulant_id_ssa_only.fillna(df.simulant_id_naive),
            nunique=lambda df: df.nunique_naive,
        )
        [['simulant_id', 'nunique']]
)
geobase_reference_file_ground_truth
    
assert set(geobase_reference_file.record_id) == set(geobase_reference_file_ground_truth.index)
    
all_ssn_simulant_pairs = pd.concat([
    census_numident.set_index("record_id")[["ssn"]].join(pd.DataFrame(census_numident_ground_truth)),
    alternate_name_numident.set_index("record_id")[["ssn"]].join(pd.DataFrame(alternate_name_numident_ground_truth)),
    alternate_dob_numident.set_index("record_id")[["ssn"]].join(pd.DataFrame(alternate_dob_numident_ground_truth)),
    name_dob_reference_file.set_index("record_id")[["ssn"]].join(pd.DataFrame(name_dob_reference_file_ground_truth)),
    geobase_reference_file.set_index("record_id")[["ssn"]].join(pd.DataFrame(geobase_reference_file_ground_truth)),
])
all_ssn_simulant_pairs
    
# The reference file records with a given SSN all have the same (primary) simulant ID
# contributing to them
assert (all_ssn_simulant_pairs.groupby('ssn').simulant_id.nunique() == 1).all()
    
ssn_to_simulant = all_ssn_simulant_pairs.groupby('ssn').simulant_id.first()
ssn_to_simulant
    
files = {
    'census_2030': (census_2030, census_2030_ground_truth),
    'census_numident': (census_numident, census_numident_ground_truth),
    'alternate_name_numident': (alternate_name_numident, alternate_name_numident_ground_truth),
    'alternate_dob_numident': (alternate_dob_numident, alternate_dob_numident_ground_truth),
    'geobase_reference_file': (geobase_reference_file, geobase_reference_file_ground_truth),
    'name_dob_reference_file': (name_dob_reference_file, name_dob_reference_file_ground_truth),
}
    
reference_files = [census_numident, alternate_name_numident, alternate_dob_numident, geobase_reference_file, name_dob_reference_file]
# TODO: Rename the ssn column to explicitly include itins, since this is confusing
all_ssns_itins_in_reference_files = pd.concat([df[["ssn"]] for df in reference_files], ignore_index=True)
ssn_to_pik = (
    all_ssns_itins_in_reference_files.drop_duplicates()
        .reset_index().rename(columns={'index': 'pik'})
        .set_index('ssn').pik
)
ssn_to_pik
    
pik_to_simulant = (
    ssn_to_simulant.reset_index()
        .assign(pik=lambda df: df.ssn.map(ssn_to_pik))
        .set_index("pik")
        .simulant_id
)
pik_to_simulant
    
import os
import shutil
from pathlib import Path

def remove_path(path):
    path = Path(path)
    if path.is_file():
        os.remove(path)
    elif path.exists():
        shutil.rmtree(path)

for file_name, (file, ground_truth) in files.items():
    # Add a unique record ID -- could do this within the pipeline, but then it's harder to match up the ground truth
    assert file.record_id.is_unique and ground_truth.index.is_unique
    assert set(file.record_id) == set(ground_truth.index)

    if file_name != 'census_2030':
        file['pik'] = file.ssn.map(ssn_to_pik)
        assert file.pik.notnull().all()

    ground_truth = ground_truth.reset_index()

    # HACK: Why does this end up having lots of ints?
    # This must be an issue in pseudopeople!
    if 'age' in file.columns:
        file['age'] = file.age.astype(str)

    file_path = Path(f'{output_dir}/{file_name}_{data_to_use}.parquet')
    remove_path(file_path)
    file.to_parquet(file_path)

    ground_truth_path = Path(f'{output_dir}/{file_name}_ground_truth_{data_to_use}.parquet')
    remove_path(ground_truth_path)
    ground_truth.to_parquet(ground_truth_path)
    
pik_to_simulant_path = Path(f'{output_dir}/pik_to_simulant_ground_truth_{data_to_use}.parquet')
remove_path(pik_to_simulant_path)
pik_to_simulant.reset_index().to_parquet(pik_to_simulant_path)
    
# Convert this notebook to a Python script
# ! cd .. && ./convert_notebook.sh generate_simulated_data/generate_simulated_data_small_sample
    
# ! date
    
