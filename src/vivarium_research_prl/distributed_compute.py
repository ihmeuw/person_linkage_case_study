import os, time
import pandas
from dataclasses import dataclass
import types
import uuid
from . import utils

def start_dask_distributed_over_slurm(
    num_jobs=3,
    cpus_per_job=2,
    # If you give dask workers more than one core, they will use it to
    # run more tasks at once, which can use more memory than is available.
    # To have more than one thread per worker but use them all for
    # multi-threading code in one task
    # at a time, you have to set cores=1, processes=1 and job_cpu > 1.
    threads_per_job=1,
    memory_per_job="10GB",
    queue='long.q',
    account='proj_simscience',
    # NOTE: This is, as Dask requests, a directory local to the compute node.
    # But IHME's cluster doesn't support this very well -- it can be small-ish,
    # full of stuff from other users, etc.
    local_directory=f"/tmp/{os.environ['USER']}_dask",
):
    import dask
    # Make Dask much less conservative with memory management: don't start spilling
    # until over 85%, don't kill until basically at memory limit
    # (I don't much mind whether Dask or slurm kills a worker)
    # We want to avoid spilling if at all possible, since it uses a resource
    # (local disk space) which is unpredictably allocated and running out of
    # it can cause the whole computation to fail
    dask.config.set({"distributed.worker.memory.target": False})
    dask.config.set({"distributed.worker.memory.spill": 0.85})
    dask.config.set({"distributed.worker.memory.pause": 0.85})
    dask.config.set({"distributed.worker.memory.terminate": 0.975})

    from dask_jobqueue import SLURMCluster

    cluster_id = uuid.uuid4()
    cluster = SLURMCluster(
        queue=queue,
        account=account,
        cores=threads_per_job,
        processes=1,
        memory=memory_per_job,
        walltime="10-00:00:00" if queue == 'long.q' else "1-00:00:00",
        # Dask distributed looks at OS-reported memory to decide whether a worker is running out.
        # If the memory allocator is not returning the memory to the OS promptly (even when holding onto it
        # is smart), it will lead Dask to make bad decisions.
        # By default, pyarrow uses jemalloc, but I could not get that to release memory quickly.
        # Even this doesn't seem to be completely working, but in combination with small-ish partitions
        # it seems to do okay -- unmanaged memory does seem to shrink from time to time, which it wasn't
        # previously doing.
        job_script_prologue=["export ARROW_DEFAULT_MEMORY_POOL=system", "export MALLOC_TRIM_THRESHOLD_=0"],
        job_cpu=cpus_per_job,
        job_name=f"dask-worker-{cluster_id}",
        local_directory=local_directory,
        # NOTE: Network file system -- probably slow and doing a lot of unnecessary I/O!
        # local_directory=f"/ihme/scratch/users/{os.environ['USER']}/dask_work_dir/dask_generate_simulated_data",
        log_directory=f"/ihme/temp/slurmoutput/{os.environ['USER']}",
        worker_extra_args=["--lifetime", "'10 days'" if queue == 'long.q' else "'1 day'", "--lifetime-stagger", "20m"],
    )

    # minimum = maximum means that it won't scale up and down for load,
    # but it will start new workers to replace failed ones.
    # We might want to experiment with scaling for load -- will the queueing
    # overhead make it not worth it?
    # https://stackoverflow.com/a/61295019
    cluster.adapt(minimum_jobs=num_jobs, maximum_jobs=num_jobs)

    # HACK: Only when running inside Singularity, I had an intermittent issue where a couple
    # workers would fail due to timeout when starting, and then the cluster would never notice
    # that those workers weren't running.
    # I do not understand what causes this; it seems like dask_jobqueue should be using squeue
    # and noticing that those job IDs are in the failed state, but it doesn't do that.
    # As a workaround, I discovered that changing the cluster's adapt settings seems to do a "reset."
    # After a few such "jiggles," it will usually succeed in bringing all the desired workers online.
    # https://github.com/dask/dask-jobqueue/issues/620
    sleeps_since_jiggle = 0
    sleep_len = 10
    num_jobs_changing = num_jobs

    while len(cluster.scheduler.workers) < num_jobs:
        num_submitted = int(os.popen(f"squeue --me -o %j | grep dask-worker-{cluster_id} | wc -l").read().strip())
        # More than three seconds per job is excessive
        if sleeps_since_jiggle > num_jobs_changing * 3 // sleep_len and num_submitted < num_jobs:
            print('Jiggling the cluster')
            cluster.adapt(minimum_jobs=num_submitted, maximum_jobs=num_submitted)
            time.sleep((num_jobs - num_submitted) * 3)
            cluster.adapt(minimum_jobs=num_jobs, maximum_jobs=num_jobs)
            num_jobs_changing = num_jobs - num_submitted
            sleeps_since_jiggle = 0

        time.sleep(sleep_len)
        sleeps_since_jiggle += 1

    from distributed import Client
    client = Client(cluster)

    return cluster, client

def start_compute_engine(compute_engine, *args, num_jobs=3, memory_per_job="10GB", threads_per_job=1, num_row_groups=None, **kwargs):
    client = None
    if compute_engine == 'pandas':
        import pandas as pd
    elif compute_engine == 'dask':
        # import dask
        # HACK: Use Python instead of pyarrow strings; this will usually be much slower and
        # require more memory, but pyarrow string columns have a 2GB max
        # Worked around this using large_strings in pyarrow instead
        # dask.config.set({"dataframe.convert-string": False})

        cluster, client = start_dask_distributed_over_slurm(*args, num_jobs=num_jobs, memory_per_job=memory_per_job, threads_per_job=threads_per_job, **kwargs)

        import dask.dataframe as pd

        display(client)
    elif compute_engine == 'dask_local':
        cluster, client = start_dask_local(*args, num_jobs=num_jobs, memory_per_job=memory_per_job, threads_per_job=threads_per_job, **kwargs)

        display(client)

        import dask.dataframe as pd
    elif compute_engine.startswith('modin'):
        if compute_engine.startswith('modin_dask_'):
            import modin.config as modin_cfg
            modin_cfg.Engine.put("dask") # Use dask instead of ray (which is the default)

            if compute_engine == 'modin_dask_distributed':
                cluster, client = start_dask_distributed_over_slurm(*args, num_jobs=num_jobs, memory_per_job=memory_per_job, threads_per_job=threads_per_job, **kwargs)
            else:
                from distributed import Client
                cpus_available = int(os.environ['SLURM_CPUS_ON_NODE'])
                client = Client(n_workers=int(cpus_available / 2), threads_per_worker=2)

            if num_row_groups is None:
                num_row_groups = 334
            # Why is this necessary?!
            # For some reason, if I don't set NPartitions, it seems to default to 0?!
            modin_cfg.NPartitions.put(num_row_groups)
            modin_cfg.MinPartitionSize.put(1_000) # ensure no column-axis partitions -- they'll need to be joined up right away anyway by our row-wise noising
            # I wish this existed!
            # modin_cfg.MaxPartitionSize.put(3_000_000)
    
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
    else:
        raise ValueError(f'Unknown compute_engine: {compute_engine}')

    return DataFrameOperations(compute_engine, pd, client, num_jobs=num_jobs, memory_per_job=memory_per_job, threads_per_job=threads_per_job), pd

@dataclass
class DataFrameOperations:
    compute_engine: str
    pd: types.ModuleType
    client: any
    num_jobs: int
    memory_per_job: str
    threads_per_job: int

    # Helpers for dealing with lazy evaluation -- Dask doesn't actually compute
    # anything until you explicitly tell it to, while Pandas and Modin are eager
    
    def persist(self, *args, wait=False):
        if len(args) == 1:
            args = args[0]
        if self.compute_engine.startswith('dask'):
            result = self.client.persist(args)
            if wait:
                import distributed
                distributed.wait(result)
            return result
        else:
            # Eagerly computed already
            return args
    
    def compute(self, *args):
        if self.compute_engine.startswith('dask'):
            import dask
            result = dask.compute(*args)
            if isinstance(result, tuple) and len(result) == 1:
                return result[0]
            else:
                return result
        else:
            # Eagerly computed already
            if len(args) == 1:
                return args[0]
            else:
                return args

    def add_unique_id_col(self, df, col_name='unique_id', value_prefix=''):
        if self.compute_engine == 'pandas' or self.compute_engine.startswith('modin'):
            return df.reset_index().rename(columns={'index': col_name}).assign(**{col_name: lambda df: value_prefix + df[col_name].astype(str)})
        elif self.compute_engine.startswith('dask'):
            # Can use cumsum as in https://stackoverflow.com/a/60852409/ if it needs
            # to be incrementing, but we just need uniqueness    
            df = df.map_partitions(add_id_to_partition, col_name=col_name, value_prefix=value_prefix, compute_engine=self.compute_engine)

            return df
        else:
            raise ValueError()

    def add_unique_record_id(self, df, dataset_name):
        return self.add_unique_id_col(df, col_name='record_id', value_prefix=f'{dataset_name}_')

    # DataFrame operations that need to be done in specific ways for Dask

    def drop_duplicates(self, df, subset=None, sort_col=None, keep='last'):
        original_columns = list(df.columns)

        if subset is None:
            subset = original_columns.copy()
        elif isinstance(subset, str):
            subset = [subset]
        else:
            subset = list(subset)

        sort_lambda = (lambda x: x.sort_values(sort_col)) if sort_col is not None else (lambda x: x)

        if self.compute_engine == 'pandas' or self.compute_engine.startswith('modin'):
            return df.pipe(sort_lambda).drop_duplicates(subset=subset, keep=keep)
        elif self.compute_engine.startswith('dask'):
            # NOTE: This approach depends crucially on https://github.com/dask/dask/issues/8437, as described in
            # https://github.com/dask/dask/issues/8437#issuecomment-983440465
            index_before = None
            if df.index.name is not None:
                index_before = df.index.name
                df = df.reset_index()

            if len(subset) == 1:
                # Cannot set_index with a column that contains any null values. Any rows that have nulls in any of subset
                # are by definition not duplicates.
                temp_index = subset[0]
                deduplicate_by_index_lambda = lambda x: x[~x.index.duplicated(keep=keep)]
                df = self.concat([
                    df[df[temp_index].isnull()],
                    df[df[temp_index].notnull()].set_index(temp_index).map_partitions(lambda x: x.pipe(sort_lambda).pipe(deduplicate_by_index_lambda).pipe(to_pyarrow_large_string)).reset_index(),
                ], ignore_index=True)
            else:
                # NOTE: This means it is best to put a high-cardinality column as the first item of subset
                temp_index = subset[0]
                # Cannot set_index with a column that contains any null values. Any rows that have nulls in any of subset
                # are by definition not duplicates.
                df = self.concat([
                    df[df[temp_index].isnull()],
                    df[df[temp_index].notnull()].set_index(temp_index).map_partitions(lambda x: x.pipe(sort_lambda).reset_index().drop_duplicates(subset=subset, keep=keep).set_index(temp_index).pipe(to_pyarrow_large_string)).reset_index(),
                ], ignore_index=True)
    
            if index_before is None:
                return df
            else:
                return df.set_index(index_before)
            # NOTE: The following is another approach I tried. It turns out that Dask groupbys don't work the way
            # you might expect for small groups, which is described more in groupby_agg_small_groups.
            # But even after working around that, it turned out to be much simpler to use the index-based approach above.
    #     elif self.compute_engine == 'dask':
    #         if sort_col is None:
    #             df = df.assign(dummy_for_cumsum=1).assign(drop_duplicates_unique_id=lambda df: df.dummy_for_cumsum.cumsum()).drop(columns=['dummy_for_cumsum'])
    #             sort_col = 'drop_duplicates_unique_id'
    
    #         if keep == 'last':
    #             to_keep = df.groupby(subset, dropna=False)[sort_col].max()
    #         elif keep == 'first':
    #             to_keep = df.groupby(subset, dropna=False)[sort_col].min()
    #         else:
    #             raise ValueError()
    
    #         result = df.merge(to_keep.to_frame(), on=(subset + [sort_col]), how='inner')[original_columns]
    
    #         if sort_col == 'drop_duplicates_unique_id':
    #             return result
    #         else:
    #             # No guarantee of uniqueness
    #             return self.drop_duplicates(result, subset=(subset + [sort_col]), keep='last')
        else:
            raise ValueError()
    
    # NOTE: Dask groupbys don't work the way you might expect for small groups.
    # In our application, when we groupby, we are usually grouping by a column (set)
    # with very high cardinality -- almost as many groups as we have rows.
    # Dask's agg function tries to create a data structure that is O(N) with the number of
    # groups on a single node, which OOMs in this situation.
    # Instead, we take advantage of https://github.com/dask/dask/issues/8437 again to turn
    # this into a P2P shuffle operation that never holds any substantial amount of the data
    # in any one place.
    # NOTE: This may be roughly the same thing as split_out=npartitions, see https://github.com/dask/dask/issues/8001,
    # but it shouldn't be any slower and I didn't know about split_out until after writing this
    def groupby_agg_small_groups(self, df, by, agg_func):
        if self.compute_engine == 'pandas' or self.compute_engine.startswith('modin'):
            return agg_func(df.groupby(by))
        elif self.compute_engine.startswith('dask'):
            if isinstance(by, str):
                by = [by]
            else:
                by = list(by)
    
            if df.index.name is not None:
                df = df.reset_index()
    
            # NOTE: This means it is best to put a high-cardinality column as the first item of by
            temp_index = by[0]
    
            # Cannot set_index with a column that contains any null values. Any rows that have nulls in any of subset
            # are not put into any group, like the default pandas behavior
            return df[df[temp_index].notnull()].set_index(temp_index).map_partitions(lambda x: agg_func(x.reset_index().groupby(by)))
        else:
            raise ValueError()
    
    def concat(self, *args, **kwargs):
        result = self.pd.concat(*args, **kwargs)
    
        if self.compute_engine.startswith('dask'):
            # By default, a Dask concat operation of A and B will lead to
            # a result with A.npartitions + B.npartitions partitions.
            # We do several operations that look like
            # df = concat([transformation_1(df), transformation_2(df)])
            # which doubles the number of partitions.
            # If we don't repartition, this doubling leads to a partition explosion,
            # which scales scheduler overhead and the memory size of the task graph.
            result = self._rebalance(result)

        return result

    def _rebalance(self, df):
        # Rebalances a dask dataframe to roughly equally-sized partitions that do not
        # exceed 10GB.
        assert self.compute_engine.startswith('dask')
        df = self._cull_empty_partitions(self.persist(df))

        # https://github.com/dask/dask/blob/91dd42529b9ecd7139926ebadbf56a8f6150991f/dask/dataframe/core.py#L8031
        mem_usages = df.map_partitions(_total_mem_usage, deep=True).compute()
        too_few = len(mem_usages) < self._optimal_num_partitions() / 5
        too_many = len(mem_usages) > self._optimal_num_partitions() * 10
        too_large = mem_usages.max() > self._max_partition_size()
        if too_few or too_many or too_large:
            print(f'Imbalanced dataframe: {too_few=}, {too_many=}, {too_large=}')
            print(mem_usages.describe())
            partition_size = self._optimal_partition_size(mem_usages.sum())
            if partition_size > mem_usages.sum() and df.npartitions == 1:
                print('Leaving as a single partition')
                return df
            elif not too_many and mem_usages.max() < partition_size * 1.5:
                return df
            else:
                print(f'Creating partitions of {partition_size / (1_000 * 1_000):,.0f}MB')
                return self.persist(df.repartition(partition_size=partition_size))
        else:
            return df

    def _optimal_partition_size(self, total_mem):
        optimal_num_partitions = self._optimal_num_partitions()

        return max(total_mem // optimal_num_partitions, 100 * 1_000 * 1_000) # Don't make smaller than 100MB

    def _optimal_num_partitions(self):
        return self.num_jobs

    def _max_partition_size(self):
        # Too large makes shuffling impossible, in addition to causing other problems like lots of
        # unmanaged memory.
        if self.memory_per_job.endswith('GB'):
            memory_per_job_b = int(self.memory_per_job.replace('GB', '')) * 1_000 * 1_000 * 1_000
        elif self.memory_per_job.endswith('MB'):
            memory_per_job_b = int(self.memory_per_job.replace('MB', '')) * 1_000 * 1_000
        elif self.memory_per_job.endswith('KB'):
            memory_per_job_b = int(self.memory_per_job.replace('KB', '')) * 1_000
        else:
            raise ValueError()

        return memory_per_job_b // 4

    def _cull_empty_partitions(self, df):
        # https://stackoverflow.com/a/50613803/
        ll = list(df.map_partitions(len).compute())
        df_delayed = df.to_delayed()
        df_delayed_new = list()
        pempty = None
        for ix, n in enumerate(ll):
            if 0 == n:
                pempty = df.get_partition(ix)
            else:
                df_delayed_new.append(df_delayed[ix])
        if pempty is not None:
            import dask
            df = dask.dataframe.from_delayed(df_delayed_new, meta=pempty)
        return df
    
    def ensure_large_string_capacity(self, df):
        if not self.compute_engine.startswith('dask'):
            # Not using pyarrow strings by default
            return df

        return df.map_partitions(
            # NOTE: In Dask they use enforce_metadata=False
            # See function definition below for an explanation
            to_pyarrow_large_string, token="to_pyarrow_large_string"
        )

    def read_parquet(self, *args, **kwargs):
        if not self.compute_engine.startswith('dask'):
            # Pass through
            return self.pd.read_parquet(*args, **kwargs)

        return self.pd.read_parquet(*args, **kwargs).pipe(self.ensure_large_string_capacity).pipe(self._rebalance)

    def to_parquet(self, df, path, *args, wait=False, **kwargs):
        # Dask doesn't overwrite if it is trying to write a directory and there is a file with
        # the same name, even with overwrite=True
        # Pandas won't overwrite if it is trying to write a file and there is a directory with
        # the same name
        utils.remove_path(path)
        r = df.to_parquet(path, *args, **kwargs)

        if wait and self.compute_engine.startswith('dask'):
            import distributed
            distributed.wait(r)

    def empty_dataframe(self, columns, dtype=None):
        dict = {col: [] for col in columns}
        if self.compute_engine.startswith('dask'):
            return self.pd.DataFrame.from_dict(dict, npartitions=1, dtype=dtype).pipe(self.ensure_large_string_capacity)
        else:
            return self.pd.DataFrame.from_dict(dict, dtype=dtype)

    def head(self, df, n=10):
        if self.compute_engine.startswith('dask'):
            # See https://stackoverflow.com/a/50524121/
            extra_kwargs = {'npartitions': -1}
        else:
            extra_kwargs = {}

        return df.head(n=n, **extra_kwargs)

def add_id_to_partition(df_part, partition_info=None, compute_engine=None, col_name=None, value_prefix=None):
    return (
        df_part
            .assign(**{col_name: range(len(df_part))})
            .assign(**{col_name: lambda x: add_strings(
                    compute_engine,
                    [
                        value_prefix,
                        str(partition_info['number'] if partition_info is not None else 0),
                        '_',
                        x[col_name].astype(str),
                    ],
                )}
            )
    )

def add_strings(compute_engine, strings):
    if not compute_engine.startswith('dask'):
        result = ''
        for string in strings:
            result += string
        return result

    # Add is not defined for large_strings!
    result = ''
    for string in strings:
        if hasattr(string, "astype"):
            result += string.fillna("").astype(str)
        else:
            result += str(string)

    return result.astype('large_string[pyarrow]')

# https://github.com/dask/dask/blob/91dd42529b9ecd7139926ebadbf56a8f6150991f/dask/dataframe/core.py#L8052-L8056
def _total_mem_usage(df, index=True, deep=False):
    mem_usage = df.memory_usage(index=index, deep=deep)
    if _is_series_like(mem_usage):
        mem_usage = mem_usage.sum()
    return mem_usage

def _is_series_like(s) -> bool:
    """Looks like a Pandas Series"""
    typ = s.__class__
    return (
        all(hasattr(typ, name) for name in ("groupby", "head", "mean"))
        and all(hasattr(s, name) for name in ("dtype", "name"))
        and "index" not in typ.__name__.lower()
    )

# NOTE: By default, Dask uses PyArrow string dtypes, not Python ones.
# This is great, because they are faster to work with, more memory efficient, and
# (crucially) nullable -- all of our string columns can be missing, which we represent
# in NumPy land with NaN, but that is finicky with Parquet.
# Unfortunately, the default PyArrow string dtype has a limit of 2GB of data per
# PyArrow "chunk," and although PyArrow chunks are supposed to work invisibly to the
# user, there are a number of bugs in PyArrow that cause common operations to try to
# switch an entire array to be a single chunk.
# See https://github.com/dask/dask/issues/10139#issuecomment-1812817180 for more on this.
# Due to these issues, we were running into the 2GB limit.
# PyArrow has a "large_string" dtype that has effectively no limit on size (64 bit instead
# of 32 bit offset).
# Dask lets us use this dtype, so long as we use it for *all* strings (due to a bug in Dask,
# see https://github.com/dask/dask/issues/10139#issuecomment-1812969372).

# Based on https://github.com/dask/dask/blob/b2f11d026d2c6f806036c050ff5dbd59d6ceb6ec/dask/dataframe/_pyarrow.py#L64-L98
# and code referenced from there
import pyarrow as pa

def is_pyarrow_string_dtype(dtype):
    """Is the input dtype a pyarrow string?"""

    pa_string_types = [pandas.StringDtype("pyarrow"), pandas.ArrowDtype(pa.string()), pandas.ArrowDtype(pa.large_string())]
    return dtype in pa_string_types

def is_pyarrow_string_index(x):
    if isinstance(x, pandas.MultiIndex):
        return any(is_pyarrow_string_index(level) for level in x.levels)
    return isinstance(x, pandas.Index) and is_pyarrow_string_dtype(x.dtype)

def to_pyarrow_large_string(df):
    string_dtype = pandas.ArrowDtype(pa.large_string())

    # Possibly convert DataFrame/Series/Index to string_dtype
    dtypes = None
    if isinstance(df, pandas.DataFrame):
        dtypes = {
            col: string_dtype for col, dtype in df.dtypes.items() if is_pyarrow_string_dtype(dtype)
        }
    elif dtype_check(df.dtype):
        dtypes = string_dtype

    if dtypes:
        df = df.astype(dtypes, copy=False)

    # Convert DataFrame/Series index too
    if is_pyarrow_string_index(df.index):
        if isinstance(df.index, pandas.MultiIndex):
            levels = {
                i: level.astype(string_dtype)
                for i, level in enumerate(df.index.levels)
                if is_pyarrow_string_dtype(level.dtype)
            }
            # set verify_integrity=False to preserve index codes
            df.index = df.index.set_levels(
                levels.values(), level=levels.keys(), verify_integrity=False
            )
        else:
            df.index = df.index.astype(string_dtype)
    return df
