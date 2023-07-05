#!/usr/bin/env python3
# coding: utf-8

# This script was automatically generated from a Jupyter notebook.
# Edit with care -- substantive edits should go in the notebook,
# or they will be overwritten the next time this script is generated.

import pseudopeople as psp
import pandas as pd, numpy as np
    
# !pip freeze | grep pseudopeople
    
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

# Initializes the source_record_ids column.
# Should only be called on "source records"; that is, records that
# come directly out of pseudopeople.
def record_id_to_single_source_record(df, source_col='record_id'):
    df = df.copy()
    df['source_record_ids'] = df[source_col].apply(lambda x: (x,))
    return df
    
# Operations that aggregate records, combining the source_record_ids column
# between all records that are aggregated into a single row

def merge_preserving_source_records(dfs, *args, **kwargs):
    dfs = [df.drop(columns=['record_id'], errors='ignore') for df in dfs]
    for df in dfs:
        assert 'source_record_ids' in df.columns

    result = dfs[0]
    for df_to_merge in dfs[1:]:
        result = (
            result.merge(df_to_merge, *args, **kwargs)
                # Get all unique source records that contributed to either record
                # Need to fill nulls here, because an outer join can cause a composite
                # record to be created only from rows in one of the dfs
                .assign(source_record_ids=lambda df: (fillna_empty_tuple(df.source_record_ids_x) + fillna_empty_tuple(df.source_record_ids_y)).apply(set).apply(tuple))
                .drop(columns=['source_record_ids_x', 'source_record_ids_y'])
        )

    return result

# Weirdly, it is quite hard to fill NaNs in a Series with
# the literal value of an empty tuple.
# See https://stackoverflow.com/a/62689667/
def fillna_empty_tuple(s):
    return s.fillna({i: tuple() for i in s.index})

import itertools

def dedupe_preserving_source_records(df, columns_to_dedupe, source_records_col='source_record_ids'):
    return (
        # NOTE: If not for dropna=False, we would silently lose
        # all rows with a null in any of the columns_to_dedupe
        df.groupby(columns_to_dedupe, dropna=False)
            # Concatenate all the tuples into one big tuple
            # https://stackoverflow.com/a/3021851/
            [source_records_col].apply(lambda x: tuple(set(itertools.chain(*x))))
            .reset_index()
    )
    
# %%time

ssa_numident = psp.generate_social_security(year=2029, config=custom_configuration)
ssa_numident = add_unique_record_id(ssa_numident, 'ssa_numident')
ssa_numident = record_id_to_single_source_record(ssa_numident)
ssa_numident
    
ssa_numident_ground_truth = ssa_numident.set_index('record_id').simulant_id
ssa_numident = ssa_numident.drop(columns=['simulant_id'])
    
tax_years = list(range(2020, 2030))
tax_years
    
# %%time

# Combine W2/1099 for all years, adding a tax_year column to track which tax year each row came from.
w2_1099 = pd.concat([
    psp.generate_taxes_w2_and_1099(year=year, config=custom_configuration).assign(tax_year=year) for year in tax_years
], ignore_index=True)
w2_1099 = add_unique_record_id(w2_1099, 'w2_1099')
w2_1099 = record_id_to_single_source_record(w2_1099)
w2_1099
    
w2_1099_ground_truth = w2_1099.set_index('record_id').simulant_id
w2_1099 = w2_1099.drop(columns=['simulant_id'])
    
# "IRS records do not contain DOB, and many of the records contain only the first four letters of the last name."
# (Brown et al. 2023, p.30, footnote 19)
# This should be updated in pseudopeople but for now we do it here.
# Note that the name part only matters for ITIN PIKing since for SSNs that are present in SSA we use name from SSA.
w2_1099 = w2_1099.drop(columns=['date_of_birth'])

PROPORTION_OF_IRS_RECORDS_WITH_TRUNCATION = 0.4 # is this a good guess at "many" in the quote above?
idx_to_truncate = w2_1099.sample(frac=PROPORTION_OF_IRS_RECORDS_WITH_TRUNCATION, random_state=1234).index
w2_1099.loc[idx_to_truncate, 'last_name'] = w2_1099.loc[idx_to_truncate, 'last_name'].str[:4]
w2_1099.loc[idx_to_truncate, 'last_name']
    
# Slightly hacky workaround for a bug in pseudopeople with the type of the PO box column
# We make sure everything is a string and remove the decimal part
# (we know the decimal point will still be there since there is no noise type that currently affects punctuation)
po_box_fixed = w2_1099.mailing_address_po_box.astype(str).str.replace('\\..*$', '', regex=True).replace('nan', np.nan)

# Floats are only present when it is NaN
assert po_box_fixed[po_box_fixed.apply(type) == float].isnull().all()
# The values that are present never contain decimals
assert not po_box_fixed[(po_box_fixed.apply(type) == str)].str.contains('.', regex=False).any()

po_box_fixed.apply(type).value_counts()
    
po_box_fixed[po_box_fixed.notnull()]
    
w2_1099['mailing_address_po_box'] = po_box_fixed
    
# %%time

census_2030 = psp.generate_decennial_census(year=2030, config=custom_configuration)
census_2030 = add_unique_record_id(census_2030, 'census_2030')
census_2030 = record_id_to_single_source_record(census_2030)
census_2030
    
census_2030_ground_truth = census_2030.set_index('record_id').simulant_id
census_2030 = census_2030.drop(columns=['simulant_id'])
    
# Similar to the above issue with PO box, there is weird type stuff with age
age_fixed = census_2030['age'].astype(str).replace('nan', np.nan)

assert age_fixed[age_fixed.apply(type) == float].isnull().all()
assert not age_fixed[age_fixed.apply(type) == str].str.contains('.', regex=False).any()

age_fixed.apply(type).value_counts()
    
age_fixed[age_fixed.notnull()]
    
census_2030['age'] = age_fixed
    
source_record_ground_truth = pd.concat([
    ssa_numident_ground_truth,
    w2_1099_ground_truth,
    census_2030_ground_truth,
])
source_record_ground_truth
    
def fill_dates(df, fill_with):
    return (
        # Replace invalid dates with nans
        pd.to_datetime(df.event_date, format='%Y%m%d', errors='coerce')
            .fillna(pd.Timestamp('2100-01-01' if fill_with == 'latest' else '1900-01-01'))
    )

def best_data_from_columns(df, columns, best_is_latest=True):
    # We don't want to throw out events with a missing/invalid date, so we'll fill them with the value *least* likely to be chosen
    # (earlier than all values if taking the latest, later than all values if taking the earliest).
    fill_with = 'earliest' if best_is_latest else 'latest'

    return (
        df
            # Without mutating the existing date column, get one that is actually
            # a date type and can be used for sorting.
            .assign(event_date_for_sort=lambda df: fill_dates(df, fill_with=fill_with))
            .sort_values('event_date_for_sort')
            .dropna(subset=columns, how='all')
            .drop_duplicates('ssn', keep=('last' if best_is_latest else 'first'))
            [['record_id', 'ssn'] + columns]
            .pipe(record_id_to_single_source_record)
    )

best_name = best_data_from_columns(
    ssa_numident,
    columns=['first_name', 'middle_initial', 'last_name'],
)

best_date_of_birth = best_data_from_columns(
    ssa_numident,
    columns=['date_of_birth'],
)

best_date_of_death = best_data_from_columns(
    ssa_numident[ssa_numident.event_type == 'death'],
    columns=['event_date'],
).rename(columns={'event_date': 'date_of_death'})

census_numident = merge_preserving_source_records(
    [best_name, best_date_of_birth, best_date_of_death],
    on='ssn',
    how='outer',
)
census_numident = add_unique_record_id(census_numident, 'census_numident')
census_numident
    
alternate_name_numident = dedupe_preserving_source_records(ssa_numident, ['ssn', 'first_name', 'middle_initial', 'last_name'])
alternate_name_numident = add_unique_record_id(alternate_name_numident, 'alternate_name_numident')
alternate_name_numident
    
alternate_name_numident.groupby('ssn').size().describe()
    
alternate_name_numident[alternate_name_numident.ssn.duplicated(keep=False)].sort_values('ssn')
    
alternate_dob_numident = dedupe_preserving_source_records(ssa_numident, ['ssn', 'date_of_birth'])
alternate_dob_numident = add_unique_record_id(alternate_dob_numident, 'alternate_dob_numident')
alternate_dob_numident
    
alternate_dob_numident.groupby('ssn').size().describe()
    
alternate_dob_numident[alternate_dob_numident.ssn.duplicated(keep=False)].sort_values('ssn')
    
name_dob_numident_records = merge_preserving_source_records(
    [alternate_name_numident, alternate_dob_numident],
    on='ssn',
    how='outer',
)
name_dob_numident_records
    
name_dob_numident_records[name_dob_numident_records.ssn.duplicated(keep=False)].sort_values('ssn')
    
# Analogous to the process of getting alternate names and dates of birth
# from SSA, we retain all versions of the name from taxes.
name_for_itins = dedupe_preserving_source_records(
    w2_1099[w2_1099.ssn.notnull() & w2_1099.ssn.str.startswith('9')],
    ['ssn', 'first_name', 'middle_initial', 'last_name'],
)
name_for_itins
    
name_for_itins.groupby('ssn').size().describe()
    
# Make sure these are disjoint sets of SSN values -- this could only happen
# if an SSN value in the Numident were corrupted into the ITIN range
assert set(name_dob_numident_records.ssn) & set(name_for_itins.ssn) == set()
    
name_dob_reference_file = pd.concat([
    name_dob_numident_records,
    name_for_itins,
], ignore_index=True)
name_dob_reference_file = add_unique_record_id(name_dob_reference_file, 'name_dob_reference_file')
name_dob_reference_file
    
address_cols = list(w2_1099.filter(like='mailing_address').columns)

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

addresses_by_ssn = dedupe_preserving_source_records(
    w2_1099.set_index(['ssn', 'source_record_ids'])
        [address_cols]
        .apply(standardize_address_part)
        .reset_index(),
    ['ssn'] + address_cols,
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
    
geobase_reference_file = merge_preserving_source_records(
    [name_dob_reference_file, addresses_by_ssn],
    on='ssn',
    how='left',
)
geobase_reference_file = add_unique_record_id(geobase_reference_file, 'geobase_reference_file')
geobase_reference_file
    
def get_simulants_for_record_ids(record_ids, ground_truth=source_record_ground_truth):
    return tuple(ground_truth.loc[record_id] for record_id in record_ids)

def get_simulants_of_source_records(df, filter_record_ids=None):
    source_record_ids = df.set_index('record_id').source_record_ids
    if filter_record_ids is not None:
        source_record_ids = source_record_ids.apply(lambda r_ids: [r_id for r_id in r_ids if filter_record_ids(r_id)])
    return source_record_ids.apply(get_simulants_for_record_ids).rename('simulant_ids')

# Working with a tuple column is a bit of a pain -- these helpers are for
# operations on this column.
# Oddly, transforming each tuple into a pandas Series (e.g. .apply(pd.Series))
# and using the pandas equivalents of these functions seems to be orders of magnitude slower.

from statistics import multimode

def nunique(data_tuple):
    return len(set(data_tuple))

def mode(data_tuple):
    if len(data_tuple) == 0:
        return None
    return multimode(data_tuple)[0]
    
census_numident_simulants = get_simulants_of_source_records(census_numident)
census_numident_simulants
    
census_numident_simulants.apply(nunique).describe()
    
census_numident_ground_truth = census_numident_simulants.apply(mode).rename('simulant_id')
census_numident_ground_truth
    
source_record_simulants = get_simulants_of_source_records(alternate_name_numident)
    
source_record_simulants.apply(nunique).describe()
    
# We take the most common ground truth value.
# Again, as shown above, there are no SSN collisions.
alternate_name_numident_ground_truth = source_record_simulants.apply(mode).rename('simulant_id')
alternate_name_numident_ground_truth
    
source_record_simulants = get_simulants_of_source_records(alternate_dob_numident)
    
source_record_simulants.apply(nunique).describe()
    
alternate_dob_numident_ground_truth = source_record_simulants.apply(mode).rename('simulant_id')
alternate_dob_numident_ground_truth
    
source_record_simulants = get_simulants_of_source_records(name_dob_reference_file)
    
source_record_simulants.apply(nunique).describe()
    
name_dob_reference_file_ground_truth = source_record_simulants.apply(mode).rename('simulant_id')
name_dob_reference_file_ground_truth
    
source_record_simulants = get_simulants_of_source_records(geobase_reference_file)
    
# Now there are some collisions, due to "borrowed SSN" noise
source_record_simulants.apply(nunique).describe()
    
# The most collisions on one SSN
most_collisions = geobase_reference_file.set_index('record_id').loc[source_record_simulants.apply(nunique).sort_values().tail(1).index].reset_index().iloc[0]
most_collisions
    
# Individual tax filings causing those collisions
w2_1099[w2_1099.record_id.isin(most_collisions.source_record_ids)]
    
# Correct value: who actually has the SSN
ssa_numident[ssa_numident.ssn == most_collisions.ssn]
    
ssa_numident_ground_truth.loc[ssa_numident[ssa_numident.ssn == most_collisions.ssn].record_id]
    
# We see here that our "mode" approach to ground truth would not be correct --
# the people borrowing the SSN outnumber the person who actually holds it
source_record_simulants.apply(mode).loc[most_collisions.record_id]
    
# So, let's prioritize the use of SSA records
source_record_simulants_based_on_ssa_only = get_simulants_of_source_records(geobase_reference_file, filter_record_ids=lambda r_id: 'ssa_numident_' in r_id)
    
geobase_reference_file_ground_truth = (
    source_record_simulants_based_on_ssa_only.apply(mode)
        # When there are no SSA records (ITIN-based), use the standard mode
        .fillna(source_record_simulants.apply(mode))
        .rename('simulant_id')
)
geobase_reference_file_ground_truth
    
geobase_reference_file_ground_truth.loc[most_collisions.record_id]
    
all_ssn_simulant_pairs = pd.concat([
    census_numident.set_index("record_id")[["ssn"]].join(census_numident_ground_truth),
    alternate_name_numident.set_index("record_id")[["ssn"]].join(alternate_name_numident_ground_truth),
    alternate_dob_numident.set_index("record_id")[["ssn"]].join(alternate_dob_numident_ground_truth),
    name_dob_reference_file.set_index("record_id")[["ssn"]].join(name_dob_reference_file_ground_truth),
    geobase_reference_file.set_index("record_id")[["ssn"]].join(geobase_reference_file_ground_truth),
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
    
for file_name, (file, ground_truth) in files.items():
    # Add a unique record ID -- could do this within the pipeline, but then it's harder to match up the ground truth
    assert file.record_id.is_unique and ground_truth.index.is_unique
    assert set(file.record_id) == set(ground_truth.index)

    # This tuple column is a pain to serialize
    file = file.drop(columns=['source_record_ids'])

    if file_name != 'census_2030':
        file['pik'] = file.ssn.map(ssn_to_pik)
        assert file.pik.notnull().all()

    ground_truth = ground_truth.reset_index()

    file.to_parquet(f'output/{file_name}_sample.parquet')
    ground_truth.to_parquet(f'output/{file_name}_ground_truth_sample.parquet')
    
pik_to_simulant.reset_index().to_parquet(f'output/pik_to_simulant_ground_truth.parquet')
    
# Convert this notebook to a Python script
# ! ../convert_notebook.sh generate_simulated_data_small_sample
    
