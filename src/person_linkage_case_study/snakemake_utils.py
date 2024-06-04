from layered_config_tree import LayeredConfigTree


# https://stackoverflow.com/a/20666342
def merge_ignoring_all(source, destination):
    """
    run me with nosetests --with-doctest file.py

    >>> a = { 'all': 'foo', 'first' : { 'all_rows' : { 'pass' : 'dog', 'number' : '1' } } }
    >>> b = { 'first' : { 'all_rows' : { 'all': 'bar', 'fail' : 'cat', 'number' : '5' } } }
    >>> merge_ignoring_all(b, a) == { 'first' : { 'all_rows' : { 'pass' : 'dog', 'fail' : 'cat', 'number' : '5' } } }
    True
    """
    for key, value in source.items():
        # Zeb modification
        if key == "all":
            continue

        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            merge_ignoring_all(value, node)
        else:
            destination[key] = value

    return destination


def merge_keys_by_level(list_of_keys_by_level_structures):
    all_levels = set()

    for list_of_keys_by_level_structure in list_of_keys_by_level_structures:
        all_levels |= list_of_keys_by_level_structure.keys()

    result = {}
    for level in all_levels:
        result[level] = set()
        for list_of_keys_by_level_structure in list_of_keys_by_level_structures:
            if level in list_of_keys_by_level_structure:
                result[level] |= list_of_keys_by_level_structure[level]

    return result


def all_possible_keys_by_level(config):
    top_level_keys = set()
    nested_keys = {}
    for k in config:
        top_level_keys |= {k}
        if isinstance(config[k], dict):
            nested_keys = merge_keys_by_level(
                [nested_keys, all_possible_keys_by_level(config[k])]
            )

    return {0: top_level_keys, **{k + 1: v for k, v in nested_keys.items()}}


def expand_all(config, level_keys=None):
    if "all" not in config:
        all_config = None
    else:
        all_config = expand_all(
            config["all"], {k - 1: v for k, v in level_keys.items() if k > 0}
        )

    if level_keys is None:
        level_keys = all_possible_keys_by_level(config)

    for k in level_keys[0]:
        if k == "all":
            continue

        if k in config and isinstance(config[k], dict):
            config[k] = expand_all(
                config[k], {k - 1: v for k, v in level_keys.items() if k > 0}
            )
            if all_config is not None:
                merge_ignoring_all(all_config, config[k])
        elif k not in config and all_config is not None:
            config[k] = all_config

    if all_config is not None:
        del config["all"]

    return config


def config_from_layers(layers):
    config = LayeredConfigTree(layers=[layer_name for layer_name, _ in layers])

    keys_by_level = {}

    for layer, layer_contents in layers:
        if "papermill_params" in layer_contents:
            keys_by_level = merge_keys_by_level(
                [
                    keys_by_level,
                    all_possible_keys_by_level(layer_contents["papermill_params"]),
                ]
            )

    for layer, layer_contents in layers:
        if "papermill_params" in layer_contents:
            layer_contents["papermill_params"] = expand_all(
                layer_contents["papermill_params"], keys_by_level
            )

        config.update(layer_contents, layer=layer)

    return config


# Snakemake requires a directory() wrapper around outputs when they are more than
# a single file.
# In our case, this depends on what compute_engine we are using -- pandas outputs
# parquet files while the distributed engines output parquet directories.
def get_directory_wrapper_if_necessary(papermill_params):
    if papermill_params["compute_engine"] == "pandas":
        return lambda x: x
    else:
        from snakemake.io import directory

        return directory


# We use papermill to run the notebooks, instead of the built-in Snakemake integration,
# because it does not generate incremental output, nor output notebooks when there is
# an error. See https://github.com/snakemake/snakemake/pull/2857
def dict_to_papermill(d):
    return " ".join([f"-p {k} {v}" for k, v in d.items()])
