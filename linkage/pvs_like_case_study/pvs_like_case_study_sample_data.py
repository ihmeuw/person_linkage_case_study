#!/usr/bin/env python3
# coding: utf-8

# This script was automatically generated from a Jupyter notebook.
# Edit with care -- substantive edits should go in the notebook,
# or they will be overwritten the next time this script is generated.

import re, copy
import pandas as pd, numpy as np
import jellyfish
    
# It's not easy to programmatically display direct dependencies separate from transitive
# dependencies.
# This environment was created by:
# $ conda create -n <name> python=3.10
# $ conda activate <name>
# $ pip install pandas numpy matplotlib pseudopeople splink jupyterlab jellyfish
# ! conda env export | grep -v 'prefix:' | tee conda_environment.yaml
    
census_2030 = pd.read_parquet('generate_simulated_data/output/census_2030_sample.parquet')
geobase_reference_file = pd.read_parquet('generate_simulated_data/output/geobase_reference_file_sample.parquet')
name_dob_reference_file = pd.read_parquet('generate_simulated_data/output/name_dob_reference_file_sample.parquet')
    
# Input file before any processing; the final result will be this with the PIK column added
census_2030_raw_input = census_2030.copy()
    
# Nickname processing
# Have not yet found a nickname list in PVS docs,
# so we do a minimal version for now -- could use
# another list such as the one in pseudopeople
# These examples all come directly from examples in the descriptions of PVS
nickname_standardizations = {
    "Bill": "William",
    "Chuck": "Charles",
    "Charlie": "Charles",
    "Cathy": "Catherine",
    "Matt": "Matthew",
}
has_nickname = census_2030.first_name.isin(nickname_standardizations.keys())
print(f'{has_nickname.sum()} nicknames in the Census')

# Add extra rows for the normalized names
census_2030 = pd.concat([
    census_2030,
    census_2030[has_nickname].assign(first_name=lambda df: df.first_name.replace(nickname_standardizations))
], ignore_index=True)

# Note: The above will introduce duplicates on record_id, so we redefine
# record_id to be unique (without getting rid of the original, input file record ID)
def add_unique_record_id(df, dataset_name):
    df = df.reset_index().rename(columns={'index': 'record_id'})
    df['record_id'] = f'{dataset_name}_' + df.record_id.astype(str)
    return df

census_2030 = add_unique_record_id(
    census_2030.rename(columns={'record_id': 'record_id_raw_input_file'}),
    "census_2030_preprocessed",
)
    
# This list of fake names comes from the NORC report, p. 100-101
# It is what was used in PVS as of 2011
with open('fake-names.txt') as f:
    fake_names = pd.Series([name.strip().upper() for name in f.readlines()])

assert (fake_names == fake_names.str.upper()).all()
fake_names
    
for col in ["first_name", "last_name"]:
    has_fake_name = census_2030[col].str.upper().isin(fake_names)
    print(f'Found {has_fake_name.sum()} fake names in {col}')
    census_2030[col] = np.where(
        has_fake_name,
        np.nan,
        census_2030[col]
    )
    
def standardize_address_part(column):
    return (
        column
            # Remove leading or trailing whitespace
            .str.strip()
            # Turn any strings of consecutive whitespace into a single space
            .str.split().str.join(' ')
            # Normalize case
            .str.upper()
            # Normalize the word street as described in the example quoted from
            # Wagner and Layne p. 9
            # In reality, there would be many rules like this
            .str.replace('\b(STREET|STR)\b', 'ST', regex=True)
            # Make sure missingness is represented consistently
            .replace('', np.nan)
    )

address_cols = ['street_number', 'street_name', 'unit_number', 'city', 'state', 'zipcode']
census_2030[address_cols] = census_2030[address_cols].apply(standardize_address_part)
    
census_2030 = census_2030[
    census_2030.first_name.notnull() |
    census_2030.last_name.notnull()
]
    
# We want to compare mailing address with physical address
geobase_reference_file = geobase_reference_file.rename(columns=lambda c: c.replace('mailing_address_', ''))
    
# PVS uses DOB as separate fields for day, month, and year
def split_dob(df):
    df = df.copy()
    # Have to be floats, because there is missingness, and we want to treat as numeric for assessing similarity
    # Note that as of now, none of our pseudopeople noise types would change the punctuation ("/") in the date, nor would
    # they insert non-numeric characters here.
    # In the future, this might happen, and this simple parsing would need more error handling.
    df[['month_of_birth', 'day_of_birth', 'year_of_birth']] = df.date_of_birth.str.split('/', expand=True).astype(float)
    return df.drop(columns=['date_of_birth'])

census_2030 = split_dob(census_2030)
geobase_reference_file = split_dob(geobase_reference_file)
name_dob_reference_file = split_dob(name_dob_reference_file)
    
# I don't fully understand the purpose of blocking on the geokey,
# as opposed to just blocking on its constituent columns.
# Maybe it is a way of dealing with missingness in those constituent
# columns (e.g. so an address with no unit number can still be blocked on geokey)?
def add_geokey(df):
    df = df.copy()
    df['geokey'] = (
        df.street_number + ' ' +
        df.street_name + ' ' +
        df.unit_number.fillna('') + ' ' +
        df.city + ' ' +
        df.state.astype(str) + ' ' +
        df.zipcode
    )
    # Normalize the whitespace -- necessary if the unit number was null
    df['geokey'] = (
        df.geokey.str.split().str.join(' ')
    )
    return df

geobase_reference_file = add_geokey(geobase_reference_file)
census_2030 = add_geokey(census_2030)
    
# Layne, Wagner, and Rothhaas p. 26: the name matching variables are
# First 15 characters First Name, First 15 characters Middle Name, First 12 characters Last Name
# Additionally, there are blocking columns for all of 1-3 initial characters of First/Last.
# We don't have a full middle name in pseudopeople (nor would that be present in a real CUF)
# so we have to stick to the first initial for middle.
def add_truncated_name_cols(df):
    df = df.copy()
    df['first_name_15'] = df.first_name.str[:15]
    df['last_name_12'] = df.last_name.str[:12]

    for num_chars in [1, 2, 3]:
        df[f'first_name_{num_chars}'] = df.first_name.str[:num_chars]
        df[f'last_name_{num_chars}'] = df.last_name.str[:num_chars]

    return df

census_2030 = add_truncated_name_cols(census_2030)
geobase_reference_file = add_truncated_name_cols(geobase_reference_file)
name_dob_reference_file = add_truncated_name_cols(name_dob_reference_file)
    
# Layne, Wagner, and Rothhaas p. 26: phonetics are used in blocking (not matching)
# - Soundex for Street Name
# - NYSIIS code for First Name
# - NYSIIS code for Last Name
# - Reverse Soundex for First Name
# - Reverse Soundex for Last Name

def add_name_phonetics(df):
    df = df.copy()

    for col in ['first_name', 'last_name']:
        df[f'{col}_nysiis'] = df[col].dropna().apply(jellyfish.nysiis)
        df[f'{col}_reverse_soundex'] = df[col].dropna().str[::-1].apply(jellyfish.soundex)

    return df

def add_address_phonetics(df):
    df = df.copy()
    df['street_name_soundex'] = df.street_name.dropna().apply(jellyfish.soundex)
    return df

census_2030 = add_name_phonetics(census_2030)
census_2030 = add_address_phonetics(census_2030)

geobase_reference_file = add_address_phonetics(geobase_reference_file)

name_dob_reference_file = add_name_phonetics(name_dob_reference_file)
    
# Columns used to "cut the database": ZIP3 and a grouping of first and last initial
def add_zip3(df):
    return df.assign(zip3=lambda x: x.zipcode.str[:3])

def add_first_last_initial_categories(df):
    # Page 20 of the NORC report: "Name-cuts are defined by combinations of the first characters of the first and last names. The twenty letter groupings
    # for the first character are: A-or-blank, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, and U-Z."
    initial_cut = lambda x: x.fillna('A').str[0].replace('A', 'A-or-blank').replace(['U', 'V', 'W', 'X', 'Y', 'Z'], 'U-Z')
    return df.assign(first_initial_cut=lambda x: initial_cut(x.first_name), last_initial_cut=lambda x: initial_cut(x.last_name))
    
census_2030 = add_zip3(census_2030)
census_2030 = add_first_last_initial_categories(census_2030)

geobase_reference_file = add_zip3(geobase_reference_file)

name_dob_reference_file = add_first_last_initial_categories(name_dob_reference_file)
    
census_2030
    
geobase_reference_file
    
name_dob_reference_file
    
def probability_two_random_records_match(input_file, reference_file):
    # NOTE: We use census_2030_raw_input instead of input_file in the numerator.
    # This is the original CUF, with only unintentional duplicates.
    # The input_file has *intentional* duplicates: records for the same person
    # with nickname and formal name.
    cuf_records = len(census_2030_raw_input)
    cartesian_product = len(input_file) * len(reference_file)
    return (cuf_records * 0.90) / cartesian_product

probability_two_random_records_match(census_2030, geobase_reference_file)
    
common_cols = [c for c in census_2030.columns if c in geobase_reference_file.columns or c in name_dob_reference_file.columns]
common_cols
    
def prep_table_for_splink(df, dataset_name, columns):
    return (
        df[[c for c in df.columns if c in columns]]
            .assign(dataset_name=dataset_name)
    )

tables_for_splink = [
    prep_table_for_splink(geobase_reference_file, "geobase_reference_file", common_cols),
    prep_table_for_splink(census_2030, "census_2030", common_cols)
]
    
[len(t) for t in tables_for_splink]
    
from splink.duckdb.linker import DuckDBLinker
from splink.duckdb.comparison_library import (
    exact_match,
    jaro_winkler_at_thresholds,
)
import splink.duckdb.comparison_level_library as cll

def numeric_column_comparison(col_name, human_name, maximum_inexact_match_difference):
    return {
        "output_column_name": col_name,
        "comparison_description": human_name,
        "comparison_levels": [
            cll.null_level(col_name),
            cll.exact_match_level(col_name),
            {
                "sql_condition": f"abs({col_name}_l - {col_name}_r) <= {maximum_inexact_match_difference}",
                "label_for_charts": "Inexact match",
            },
            cll.else_level(),
        ],
    }

numeric_columns = ["day_of_birth", "month_of_birth", "year_of_birth"]

settings = {
    "link_type": "link_only",
    "comparisons": [
        jaro_winkler_at_thresholds("first_name_15", 750 / 900),
        jaro_winkler_at_thresholds("last_name_12", 750 / 900),
        exact_match("middle_initial"),
        numeric_column_comparison("day_of_birth", "Day of birth", maximum_inexact_match_difference=5),
        numeric_column_comparison("month_of_birth", "Month of birth", maximum_inexact_match_difference=3),
        numeric_column_comparison("year_of_birth", "Year of birth", maximum_inexact_match_difference=5),
        # Using same cutoffs as for names, in the absence of a better description of
        # how these are compared
        jaro_winkler_at_thresholds("street_number", 750 / 900),
        jaro_winkler_at_thresholds("street_name", 750 / 900),
        jaro_winkler_at_thresholds("unit_number", 750 / 900),
        jaro_winkler_at_thresholds("zipcode", 750 / 900),
    ],
    "probability_two_random_records_match": probability_two_random_records_match(census_2030, geobase_reference_file),
    "unique_id_column_name": "record_id",
    # Not sure exactly what this does, but it is necessary for some of the fancier graphs below
    "retain_intermediate_calculation_columns": True,
}

linker = DuckDBLinker(
    tables_for_splink,
    settings,
    input_table_aliases=["reference_file", "census_2030"]
)
    
# %%time

linker.estimate_u_using_random_sampling(max_pairs=1e7, seed=1234)
    
# Ignore the green bars on the left, these are the m probabilities that haven't been estimated yet
linker.m_u_parameters_chart()
    
blocking_rule_for_training = "l.first_name_15 = r.first_name_15"
em_session_1 = linker.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training,
    # Fix lambda; u is fixed by default
    fix_probability_two_random_records_match=True,
)
    
em_session_1.m_u_values_interactive_history_chart()
    
em_session_1.match_weights_interactive_history_chart()
    
blocking_rule_for_training = "l.middle_initial = r.middle_initial and l.last_name_12 = r.last_name_12"
em_session_2 = linker.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training,
    # Fix lambda; u is fixed by default
    fix_probability_two_random_records_match=True,
)
    
em_session_2.m_u_values_interactive_history_chart()
    
linker.match_weights_chart()
    
linker.m_u_parameters_chart()
    
linker.parameter_estimate_comparisons_chart()
    
splink_settings = linker._settings_obj.as_dict()
    
# Save these variables; this means that if you restart the kernel, you don't need to run this first part of the notebook again.
# %store splink_settings
    
from dataclasses import dataclass

# Calculate this once to save time -- mapping from record_id to record_id_raw_input_file
# There can be multiple records (with different record_id) for the same input file record
# (record_id_raw_input_file) because of the handling of nicknames by creating extra records.
record_id_raw_input_file_by_record_id = census_2030.set_index('record_id').record_id_raw_input_file

all_piks = pd.concat([
    name_dob_reference_file[["record_id", "pik"]],
    geobase_reference_file[["record_id", "pik"]]
], ignore_index=True).set_index("record_id").pik

dates_of_death = (
    pd.read_parquet('generate_simulated_data/output/census_numident_sample.parquet')
        .set_index('pik')
        .date_of_death
        .pipe(lambda s: pd.to_datetime(s, format='%Y%m%d', errors='coerce'))
)

class PVSCascade:
    def __init__(self):
        # This dataframe will accumulate the PIKs to attach to the input file
        self.confirmed_piks = pd.DataFrame(columns=["record_id_raw_input_file", "pik"])
        self.current_module = None

    def start_module(self, *args, **kwargs):
        assert self.current_module is None or self.current_module.confirmed
        self.current_module = PVSModule(*args, **kwargs)

    def run_matching_pass(self, *args, **kwargs):
        self.current_module.run_matching_pass(*args, already_confirmed_piks=self.confirmed_piks, **kwargs)

    def confirm_piks(self, *args, **kwargs):
        # Make sure we are not about to confirm PIKs for any of the same files we have
        # already PIKed
        assert (
            set(self.current_module.provisional_links.record_id_raw_input_file) &
            set(self.confirmed_piks.record_id_raw_input_file)
        ) == set()

        newly_confirmed_piks = self.current_module.confirm_piks_from_provisional_links()

        self.confirmed_piks = pd.concat([
            self.confirmed_piks,
            newly_confirmed_piks,
        ], ignore_index=True)

        return self.confirmed_piks

@dataclass
class PVSModule:
    name: str
    reference_file: pd.DataFrame
    reference_file_name: str
    cut_columns: list[str]
    matching_columns: list[str]

    def __post_init__(self):
        self.provisional_links = pd.DataFrame(columns=["record_id_census_2030"])
        self.confirmed = False

    def run_matching_pass(
        self,
        pass_name,
        blocking_columns,
        probability_threshold=0.995,
        input_data_transformation=lambda x: x,
        already_confirmed_piks=pd.DataFrame(columns=["record_id_raw_input_file"]),
    ):
        assert self.confirmed == False

        print(f"Running {pass_name} of {self.name}")

        columns_needed = ["record_id"] + self.cut_columns + blocking_columns + self.matching_columns
        tables_for_splink = [
            prep_table_for_splink(self.reference_file, self.reference_file_name, columns_needed),
            prep_table_for_splink(
                census_2030[
                    # Only look for matches among records that have not received a confirmed PIK
                    ~census_2030.record_id_raw_input_file.isin(already_confirmed_piks.record_id_raw_input_file) &
                    # Only look for matches among records that have not received a provisional link
                    # NOTE: "records" here does not mean input file records -- a nickname record having
                    # a provisional link does not prevent a canonical name record from the same input record
                    # from continuing to match
                    ~census_2030.record_id.isin(self.provisional_links.record_id_census_2030)
                ].pipe(input_data_transformation),
                "census_2030",
                columns_needed,
            )
        ]
        print(f"Files to link are {len(tables_for_splink[0]):,.0f} and {len(tables_for_splink[1]):,.0f} records")

        blocking_rule_parts = [f"l.{col} = r.{col}" for col in self.cut_columns + blocking_columns]
        blocking_rule = " and ".join(blocking_rule_parts)

        # We base our Splink linker for this pass on the one we trained above,
        # but limiting it to the relevant column comparisons and updating the pass-specific
        # settings
        pass_splink_settings = copy.deepcopy(splink_settings)
        pass_splink_settings["comparisons"] = [
            c for c in pass_splink_settings["comparisons"] if c["output_column_name"] in self.matching_columns
        ]
        pass_splink_settings["probability_two_random_records_match"] = (
            probability_two_random_records_match(census_2030, self.reference_file)
        )
        pass_splink_settings["blocking_rules_to_generate_predictions"] = [blocking_rule]
    
        linker = DuckDBLinker(
            tables_for_splink,
            pass_splink_settings,
            # Must match order of tables_for_splink
            input_table_aliases=["reference_file", "census_2030"]
        )
    
        num_comparisons = linker.count_num_comparisons_from_blocking_rule(blocking_rule)
        print(f"Number of pairs that will be compared: {num_comparisons:,.0f}")
    
        # https://moj-analytical-services.github.io/splink/demos/06_Visualising_predictions.html#comparison-viewer-dashboard
        # We also include some pairs below the threshold, for additional context.
        pairs_worth_inspecting = linker.predict(threshold_match_probability=probability_threshold - 0.2)
    
        dashboard_file_name = f"splink_temp/{self.name.replace(' ', '_')}__{pass_name.replace(' ', '_')}.html"
        linker.comparison_viewer_dashboard(pairs_worth_inspecting, dashboard_file_name, overwrite=True)
        from IPython.display import IFrame, display
        display(IFrame(
            src=f"./{dashboard_file_name}", width="100%", height=1200
        ))
    
        new_provisional_links = (
            pairs_worth_inspecting
                .as_pandas_dataframe()
                .pipe(lambda df: df[df.match_probability >= probability_threshold])
        )
        if len(new_provisional_links) > 0:
            new_provisional_links = label_pairs_with_dataset(new_provisional_links)
            new_provisional_links["record_id_raw_input_file"] = (
                new_provisional_links.record_id_census_2030.map(record_id_raw_input_file_by_record_id)
            )
        
            self.provisional_links = pd.concat([
                self.provisional_links,
                new_provisional_links.assign(module_name=self.name, pass_name=pass_name)
            ], ignore_index=True)
    
        still_eligible = (
            (~census_2030.record_id_raw_input_file.isin(already_confirmed_piks.record_id_raw_input_file)) &
            (~census_2030.record_id.isin(self.provisional_links.record_id_census_2030))
        )
        print(f'Matched {len(new_provisional_links)} records; {still_eligible.mean():.2%} still eligible to match')

    def confirm_piks_from_provisional_links(self):
        assert not self.confirmed

        provisional_links = self.provisional_links
        provisional_links["pik"] = provisional_links.record_id_reference_file.map(all_piks)

        # "After the initial set of links is created in GeoSearch, a post-search program is run to determine
        # which of the links are retained. A series of checks are performed: First the date of death
        # information from the Numident is checked and links to deceased persons are dropped. Next a
        # check is made for more than one SSN assigned to a source record. If more than one SSN is
        # assigned, the best link is selected based on match weights. If no best SSN is determined, all SSNs
        # assigned in the GeoSearch module are dropped and the input record cascades to the next
        # module. A similar post-search program is run at the end of all search modules."
        # - Layne et al. p. 5

        # Drop links to deceased people
        # NOTE: On p. 38 of Brown et al. (2023) it discusses at length the number of PVS matches to deceased
        # people, which should not be possible based on this process.
        # Even though this is more recent, I can't think of a reason why this check would have
        # been *removed* from PVS -- can we chalk this up to something experimental they were doing for
        # the AR Census in the 2023 report?
        link_dates_of_death = provisional_links["pik"].map(dates_of_death)
        # Census day 2030
        deceased_links = link_dates_of_death <= pd.to_datetime("2030-04-01")
        print(f'{deceased_links.sum()} input records linked to deceased people, dropping links')
        provisional_links = provisional_links[~deceased_links]

        # Check for multiple linkage to a single input file record
        max_probability = provisional_links.groupby("record_id_raw_input_file").match_probability.max()
        piks_per_input_file = (
            provisional_links.groupby("record_id_raw_input_file")
                .apply(lambda df: df[df.match_probability == max_probability[df.name]].pik.nunique())
        )

        multiple_piks = piks_per_input_file[piks_per_input_file > 1].index
        print(f'{len(multiple_piks)} input records linked to multiple PIKs, dropping links')
        provisional_links = (
            provisional_links[~provisional_links.record_id_raw_input_file.isin(multiple_piks)]
                .sort_values("match_probability")
                .groupby("record_id_raw_input_file")
                .last()
                .reset_index()
        )

        assert (provisional_links.groupby("record_id_raw_input_file").pik.nunique() == 1).all()

        self.confirmed = True
        self.provisional_links = None
        
        return (
            provisional_links[[
                "record_id_raw_input_file",
                "record_id_census_2030",
                "record_id_reference_file",
                "pik",
                "module_name",
                "pass_name",
                "match_probability",
            ]]
        )

def label_pairs_with_dataset(pairs):
    # Name the columns according to the datasets, not "r" (right) and "l" (left)
    for suffix in ["r", "l"]:
        pairs = (
            pairs.groupby(f'source_dataset_{suffix}')
                .apply(lambda df_g: replace_suffix_with_source_dataset(df_g, suffix, df_g.name))
        )

    return pairs

def replace_suffix_with_source_dataset(df, suffix, source_dataset):
    return df.rename(columns=lambda c: re.sub(f'_{suffix}$', f'_{source_dataset}', c))
    
pvs_cascade = PVSCascade()
    
pvs_cascade.start_module(
    name="geosearch",
    reference_file=geobase_reference_file,
    reference_file_name="geobase_reference_file",
    cut_columns=["zip3"],
    matching_columns=[
        "first_name_15",
        "last_name_12",
        "middle_initial",
        "day_of_birth",
        "month_of_birth",
        "year_of_birth",
        "street_number",
        "street_name",
        "unit_number",
        "zipcode",
    ],
)
    
pvs_cascade.run_matching_pass(
    pass_name="geokey",
    blocking_columns=["geokey"],
)
    
def switch_first_and_last_names(df):
    return (
        df.rename(columns={"first_name": "last_name", "last_name": "first_name"})
            # Re-calculate the truncated versions of first and last.
            # NOTE: It is not necessary to re-calculate the phonetic versions, because
            # those are never used in any pass that has a name switch.
            .pipe(add_truncated_name_cols)
    )
    
# We don't actually have any swapping of first and last names in pseudopeople
pvs_cascade.run_matching_pass(
    pass_name="geokey name switch",
    blocking_columns=["geokey"],
    input_data_transformation=switch_first_and_last_names,
)
    
pvs_cascade.run_matching_pass(
    pass_name="house number and street name Soundex",
    blocking_columns=["street_number", "street_name_soundex"],
)
    
pvs_cascade.run_matching_pass(
    pass_name="house number and street name Soundex name switch",
    blocking_columns=["street_number", "street_name_soundex"],
    input_data_transformation=switch_first_and_last_names,
)
    
pvs_cascade.run_matching_pass(
    pass_name="some name and DOB information",
    blocking_columns=["first_name_2", "last_name_2", "year_of_birth"],
)
    
pvs_cascade.confirm_piks()
    
pvs_cascade.confirmed_piks.groupby(["module_name", "pass_name"]).size().sort_values(ascending=False)
    
pvs_cascade.start_module(
    name="namesearch",
    reference_file=name_dob_reference_file,
    reference_file_name="name_dob_reference_file",
    cut_columns=["first_initial_cut", "last_initial_cut"],
    matching_columns=[
        "first_name_15",
        "last_name_12",
        "middle_initial",
        "day_of_birth",
        "month_of_birth",
        "year_of_birth",
    ],
)
    
pvs_cascade.run_matching_pass(
    pass_name="DOB and NYSIIS of name",
    blocking_columns=["day_of_birth", "month_of_birth", "year_of_birth", "first_name_nysiis", "last_name_nysiis"],
)
    
pvs_cascade.run_matching_pass(
    pass_name="DOB and initials",
    blocking_columns=["day_of_birth", "month_of_birth", "year_of_birth", "first_name_1", "last_name_1"],
)
    
pvs_cascade.run_matching_pass(
    pass_name="year of birth and first two characters of name",
    blocking_columns=["year_of_birth", "first_name_2", "last_name_2"],
)
    
pvs_cascade.run_matching_pass(
    pass_name="birthday and first two characters of name",
    blocking_columns=["day_of_birth", "month_of_birth", "first_name_2", "last_name_2"],
)
    
pvs_cascade.confirm_piks()
    
pvs_cascade.confirmed_piks.groupby(["module_name", "pass_name"]).size().sort_values(ascending=False)
    
pvs_cascade.start_module(
    name="dobsearch",
    reference_file=name_dob_reference_file,
    reference_file_name="name_dob_reference_file",
    cut_columns=["day_of_birth", "month_of_birth"],
    matching_columns=[
        "first_name_15",
        "last_name_12",
        "middle_initial",
        "day_of_birth",
        "month_of_birth",
        "year_of_birth",
    ],
)
    
pvs_cascade.run_matching_pass(
    pass_name="initials name switch",
    blocking_columns=["first_name_1", "last_name_1"],
    input_data_transformation=switch_first_and_last_names,
)
    
pvs_cascade.run_matching_pass(
    pass_name="first three characters of name",
    blocking_columns=["first_name_3", "last_name_3"],
)
    
pvs_cascade.run_matching_pass(
    pass_name="reverse Soundex of name",
    blocking_columns=["first_name_reverse_soundex", "last_name_reverse_soundex"],
)
    
pvs_cascade.run_matching_pass(
    pass_name="first two characters of first name and year of birth",
    blocking_columns=["first_name_2", "year_of_birth"],
)
    
pvs_cascade.confirm_piks()
    
pvs_cascade.confirmed_piks.groupby(["module_name", "pass_name"]).size().sort_values(ascending=False)
    
# TODO: As of now in pseudopeople, our only indicator in the Census data of household
# is the geokey itself. This can be messed up by noise, so we should switch to using
# a (presumably low-noise) household indicator when we have that.
household_id_approximation = (
    census_2030[["geokey"]].drop_duplicates().reset_index()
        .rename(columns={"index": "household_id"})
        .set_index("geokey").household_id
)
census_2030["household_id"] = census_2030.geokey.map(household_id_approximation)

piks_with_household = (
    census_2030[["household_id", "record_id_raw_input_file"]]
        .merge(pvs_cascade.confirmed_piks, on="record_id_raw_input_file", how="left")
)
someone_piked = piks_with_household[piks_with_household.pik.notnull()].groupby("household_id").pik.nunique() > 0
someone_unpiked = piks_with_household[piks_with_household.pik.isnull()].groupby("household_id").pik.nunique() > 0
    
eligible_households = someone_piked & someone_unpiked
    
piks_by_household = piks_with_household[["household_id", "pik"]].dropna(subset="pik").drop_duplicates()
piks_by_household
    
geokeys_by_household = (
    piks_by_household
        .merge(geobase_reference_file[["pik", "geokey"]].dropna(subset="geokey"), on="pik")
        .drop(columns=["pik"])
        .drop_duplicates()
)
geokeys_by_household
    
records_to_search_by_household = geokeys_by_household.merge(geobase_reference_file, on="geokey")
records_to_search_by_household
    
# Apparently, we exclude from the reference file all *reference file* records with a PIK that
# has already been assigned to an input file row.
# Doing this goes against the normal assumption, which is that reference file records can match
# to multiple input file records.
# This is really surprising to me, but it seems clear from "the program
# removes all household members with a PIK, leaving the unPIKed persons in the
# household. This becomes the reference file to search against." (Wagner and Layne, p. 16)
hhcomp_reference_file = records_to_search_by_household[~records_to_search_by_household.pik.isin(pvs_cascade.confirmed_piks.pik)]
hhcomp_reference_file
    
pvs_cascade.start_module(
    name="hhcompsearch",
    reference_file=hhcomp_reference_file,
    reference_file_name="hhcomp_reference_file",
    cut_columns=["household_id"],
    matching_columns=[
        "first_name_15",
        "last_name_12",
        "middle_initial",
        "day_of_birth",
        "month_of_birth",
        "year_of_birth",
    ],
)
    
pvs_cascade.run_matching_pass(
    pass_name="initials",
    blocking_columns=["first_name_1", "last_name_1"],
)
    
pvs_cascade.run_matching_pass(
    pass_name="year of birth",
    blocking_columns=["year_of_birth"],
)
    
pvs_cascade.confirm_piks()
    
pvs_cascade.confirmed_piks.groupby(["module_name", "pass_name"]).size().sort_values(ascending=False)
    
pvs_cascade.confirmed_piks
    
pik_values = (
    pvs_cascade.confirmed_piks
        .rename(columns={"record_id_raw_input_file": "record_id"})[["record_id", "pik"]]
        .drop_duplicates()
)
    
census_2030_piked = census_2030_raw_input.copy()
census_2030_piked = census_2030_piked.merge(
    pik_values,
    how="left",
    on="record_id",
    validate="1:1",
)
census_2030_piked
    
piked_proportion = census_2030_piked.pik.notnull().mean()
# Compare with 90.28% of input records PIKed in the 2010 CUF,
# as reported in Wagner and Layne, Table 2, p. 18 
print(f'{piked_proportion:.2%} of the input records were PIKed')
    
census_2030_piked.to_parquet('census_2030_piked_sample.parquet')
    
# All modules, Medicare database, calculated from Layne, Wagner, and Rothhaas Table 1 (p. 15)
real_life_pvs_accuracy = 1 - (2_585 + 60_709 + 129_480 + 89_094) / (52_406_981 + 5_170_924 + 49_374_794 + 50_327_034)
f'{real_life_pvs_accuracy:.5%}'
    
census_2030_ground_truth = (
    pd.read_parquet('generate_simulated_data/output/census_2030_ground_truth_sample.parquet')
        .set_index('record_id').simulant_id
)
    
reference_files_ground_truth = pd.concat([
    pd.read_parquet('generate_simulated_data/output/geobase_reference_file_ground_truth_sample.parquet'),
    pd.read_parquet('generate_simulated_data/output/name_dob_reference_file_ground_truth_sample.parquet'),
], ignore_index=True).set_index('record_id').simulant_id
reference_files_ground_truth
    
possible_to_pik_proportion = census_2030_ground_truth.isin(reference_files_ground_truth).mean()
print(
    f'{(1 - possible_to_pik_proportion):.2%} of the input records are '
    'impossible to PIK correctly, since they are not in any reference files'
)
    
print(
    f'Assigned PIKs to {(piked_proportion / possible_to_pik_proportion):.2%} of PIK-able records'
)
    
# Multiple Census rows assigned the same PIK, indicating the model thinks they are duplicates in Census
census_2030_piked.pik.value_counts().value_counts()
    
# However, in this version of pseudopeople, there are no actual duplicates in Census
assert not census_2030_ground_truth.duplicated().any()
    
# Interesting: in pseudopeople, sometimes siblings are assigned the same (common) first name, making them almost identical.
# The only giveaway is their age and DOB.
# Presumably, this tends not to happen in real life.
duplicate_piks = census_2030_piked.pik.value_counts()[census_2030_piked.pik.value_counts() > 1].index
census_2030_piked[census_2030_piked.pik.isin(duplicate_piks)].sort_values('pik')
    
pik_to_simulant_ground_truth = (
    pd.read_parquet('generate_simulated_data/output/pik_to_simulant_ground_truth.parquet')
        .set_index("pik").simulant_id
)
pik_to_simulant_ground_truth
    
matched_simulant_id = census_2030_piked.set_index("record_id").pik.map(pik_to_simulant_ground_truth)
    
census_2030_ground_truth[matched_simulant_id.notnull()]
    
piks_correct_proportion = (matched_simulant_id[matched_simulant_id.notnull()] == census_2030_ground_truth[matched_simulant_id.notnull()]).mean()
print(f'{piks_correct_proportion:.5%} of the PIKs assigned were correct; compare with {real_life_pvs_accuracy:.5%} in real life')
    
confirmed_piks_with_ground_truth = pvs_cascade.confirmed_piks.copy()
confirmed_piks_with_ground_truth["correct"] = (
    confirmed_piks_with_ground_truth.record_id_raw_input_file
        .map(census_2030_ground_truth)
    ==
    confirmed_piks_with_ground_truth.record_id_reference_file
        .map(reference_files_ground_truth)
)
confirmed_piks_with_ground_truth.correct.mean()
    
# Accuracy by module -- note that this shows the opposite pattern (with the sample data)
# relative to the results of Layne et al., who found GeoSearch was much *more* accurate
confirmed_piks_with_ground_truth.groupby(["module_name"]).correct.mean().sort_values()
    
# Accuracy by pass -- could be used to tune pass-specific cutoffs, but
# this might not be too informative while we are still using the sample data.
confirmed_piks_with_ground_truth.groupby(["module_name", "pass_name"]).correct.mean().sort_values()
    
(
    confirmed_piks_with_ground_truth[~confirmed_piks_with_ground_truth.correct]
        .groupby(["module_name", "pass_name"])
        .size().sort_values()
)
    
incorrectly_linked_pairs = (
    confirmed_piks_with_ground_truth[~confirmed_piks_with_ground_truth.correct]
        [["record_id_census_2030", "record_id_reference_file"]].drop_duplicates()
)
incorrectly_linked_pairs
    
census_incorrectly_linked = (
    census_2030
        .set_index("record_id")
        .loc[incorrectly_linked_pairs["record_id_census_2030"], :]
        .reset_index()
)

reference_file_incorrectly_linked = (
    pd.concat([geobase_reference_file, name_dob_reference_file], ignore_index=True)
        .set_index("record_id")
        .loc[incorrectly_linked_pairs["record_id_reference_file"], :]
        .reset_index()
)

comparison_cols = [
    "first_name",
    "middle_initial",
    "last_name",
    "year_of_birth",
    "month_of_birth",
    "day_of_birth",
    "geokey",
]

census_incorrectly_linked[comparison_cols].compare(
    reference_file_incorrectly_linked[comparison_cols],
    keep_shape=True,
    keep_equal=True,
)
    
census_2030_piked.to_parquet('census_2030_piked_sample.parquet')
    
# Convert this notebook to a Python script
# ! ./convert_notebook.sh pvs_like_case_study_sample_data
    
