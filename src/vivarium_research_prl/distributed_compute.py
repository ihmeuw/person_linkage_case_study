import glob
import os, time, datetime, pathlib, re, atexit
import asyncio
import requests
from typing import Any, Dict, Literal
import pandas
from dataclasses import dataclass
import types
import uuid
from . import utils

def start_dask_distributed_over_slurm(
    num_workers: int,
    local_directory: str | pathlib.Path,
    log_directory: str | pathlib.Path = None,
    # If you give dask workers more than one core, they will use it to
    # run more tasks at once, which can use more memory than is available.
    # To have more than one thread per worker but use them all for
    # multi-threading code in one task
    # at a time, you have to set cores=1, processes=1 and job_cpu > 1.
    cpus_per_worker: int = 1,
    threads_per_worker: int = 1,
    memory_per_worker: str = "10GB",
    worker_walltime: str = None,
    memory_constrained: bool = True,
    scheduler: Literal["slurm", "htcondor", "lsf", "moab", "oar", "pbs", "sge"] = "slurm",
    **extra_scheduler_kwargs,
):
    
    import dask
    # I don't much mind whether Dask or the scheduler kills a worker
    dask.config.set({"distributed.worker.memory.terminate": 0.975})
    if not memory_constrained:
        # Make Dask much less conservative with memory management: don't start spilling
        # until over 85%, don't kill until basically at memory limit
        # This works well as long as the computation isn't under substantial memory pressure.
        # If it is, this can cause entire workers to die, which is very expensive
        # (requires everything that worker was holding to be re-computed).
        dask.config.set({"distributed.worker.memory.target": False})
        dask.config.set({"distributed.worker.memory.spill": 0.85})
        dask.config.set({"distributed.worker.memory.pause": 0.85})

        # We'll be cutting it closer with memory usage, so it is important to trim promptly.
        # Dask distributed looks at OS-reported memory to decide whether a worker is running out.
        # If the memory allocator is not returning the memory to the OS promptly (even when holding onto it
        # is smart), it will lead Dask to make bad decisions.
        # By default, pyarrow uses jemalloc, but I could not get that to release memory quickly.
        # Even this doesn't seem to be completely working, but in combination with small-ish partitions
        # it seems to do okay -- unmanaged memory does seem to shrink from time to time, which it wasn't
        # previously doing.
        job_script_prologue = ["export ARROW_DEFAULT_MEMORY_POOL=system", "export MALLOC_TRIM_THRESHOLD_=0"]
    else:
        job_script_prologue = None

    import dask_jobqueue

    if worker_walltime is not None:
        if isinstance(worker_walltime, int):
            walltime_minutes = worker_walltime
        elif isinstance(worker_walltime, datetime.timedelta):
            walltime_minutes = int(worker_walltime.total_seconds() / 60.0)
        else:
            # Roughly re-implement Slurm's timedelta format, which is not supported by Dask
            match = re.match(r"^((((?P<days>\d+)-)?(?P<hours>\d+):)?(?P<minutes>\d+):)?(?P<seconds>\d+)$", worker_walltime)
            parts = {
                key: int(match.group(key))
                for key in ['days', 'hours', 'minutes', 'seconds']
                if match.group(key) is not None
            }
            assert match is not None
            walltime = datetime.timedelta(**parts)
            walltime_minutes = int(walltime.total_seconds() / 60.0)

        worker_extra_args = [
            "--lifetime", f"'{walltime_minutes}m'",
            "--lifetime-stagger", "20m",
        ]
    else:
        walltime_minutes = None
        worker_extra_args = None

    cluster_id = uuid.uuid4()

    cluster_args = {
        'cores': threads_per_worker,
        'processes': 1,
        'memory': memory_per_worker,
        'job_script_prologue': job_script_prologue,
        'job_name': f"dask-worker-{cluster_id}",
        'local_directory': local_directory,
        'log_directory': log_directory,
        'worker_extra_args': worker_extra_args,
    }

    # HTCondor does not have a walltime concept
    if scheduler != 'htcondor':
        cluster_args['walltime'] = worker_walltime

    # Only the Slurm part of dask_jobqueue supports CPU
    # allocation
    if scheduler != 'slurm':
        assert cpus_per_worker == 1

    # NOTE: All of these besides Slurm are untested!
    if scheduler == 'slurm':
        cluster = dask_jobqueue.SLURMCluster(
            **cluster_args,
            job_cpu=cpus_per_worker,
            **extra_scheduler_kwargs,
        )
    elif scheduler == 'htcondor':
        cluster = dask_jobqueue.HTCondorCluster(
            **cluster_args,
            **extra_scheduler_kwargs,
        )
    elif scheduler == 'lsf':
        cluster = dask_jobqueue.LSFCluster(
            **cluster_args,
            **extra_scheduler_kwargs,
        )
    elif scheduler == 'moab':
        cluster = dask_jobqueue.MoabCluster(
            **cluster_args,
            **extra_scheduler_kwargs,
        )
    elif scheduler == 'oar':
        cluster = dask_jobqueue.OARCluster(
            **cluster_args,
            **extra_scheduler_kwargs,
        )
    elif scheduler == 'pbs':
        cluster = dask_jobqueue.PBSCluster(
            **cluster_args,
            **extra_scheduler_kwargs,
        )
    elif scheduler == 'sge':
        cluster = dask_jobqueue.SGECluster(
            **cluster_args,
            **extra_scheduler_kwargs,
        )
    else:
        raise ValueError(f'Unknown scheduler {scheduler}')

    # minimum = maximum means that it won't scale up and down for load,
    # but it will start new workers to replace failed ones.
    # We might want to experiment with scaling for load -- will the queueing
    # and data transfer overhead make it not worth it?
    # https://stackoverflow.com/a/61295019
    cluster.adapt(minimum_jobs=num_workers, maximum_jobs=num_workers)

    if scheduler == 'slurm':
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
        num_jobs_changing = num_workers

        while len(cluster.scheduler.workers) < num_workers:
            num_submitted = int(os.popen(f"squeue --me -o %j | grep dask-worker-{cluster_id} | wc -l").read().strip())
            # More than three seconds per job is excessive
            if sleeps_since_jiggle > num_jobs_changing * 3 // sleep_len and num_submitted < num_workers:
                print('Jiggling the cluster')
                cluster.adapt(minimum_jobs=num_submitted, maximum_jobs=num_submitted)
                time.sleep((num_workers - num_submitted) * 3)
                cluster.adapt(minimum_jobs=num_workers, maximum_jobs=num_workers)
                num_jobs_changing = num_workers - num_submitted
                sleeps_since_jiggle = 0

            time.sleep(sleep_len)
            sleeps_since_jiggle += 1

    from distributed import Client
    client = Client(cluster)

    return cluster, client

def start_dask_local(
    num_workers,
    threads_per_worker,
    memory_per_worker,
    local_directory,
    **kwargs,
):
    from dask.distributed import LocalCluster
    import dask
    dask.config.set({"distributed.worker.memory.terminate": False})
    dask.config.set({"distributed.scheduler.worker-ttl": None})
    dask.config.set({"distributed.comm.retry.count": 20})
    dask.config.set({"distributed.comm.timeouts.connect": 5 * 60})
    dask.config.set({"distributed.comm.timeouts.tcp": 5 * 60})

    cluster = LocalCluster(
        n_workers=num_workers,
        memory_limit=memory_per_worker,
        threads_per_worker=threads_per_worker,
        local_directory=local_directory,
        **kwargs,
    )
    client = cluster.get_client()

    return cluster, client

def start_compute_engine(compute_engine, num_workers=3, memory_per_worker="10GB", threads_per_worker=1, num_row_groups=None, **kwargs):
    client = None
    if compute_engine == 'pandas':
        import pandas as pd
    elif compute_engine == 'dask':
        # import dask
        # HACK: Use Python instead of pyarrow strings; this will usually be much slower and
        # require more memory, but pyarrow string columns have a 2GB max
        # Worked around this using large_strings in pyarrow instead
        # dask.config.set({"dataframe.convert-string": False})

        cluster, client = start_dask_distributed_over_slurm(num_workers=num_workers, memory_per_worker=memory_per_worker, threads_per_worker=threads_per_worker, **kwargs)

        import dask.dataframe as pd

        display(client)
    elif compute_engine == 'dask_local':
        cluster, client = start_dask_local(num_workers=num_workers, memory_per_worker=memory_per_worker, threads_per_worker=threads_per_worker, **kwargs)

        display(client)

        import dask.dataframe as pd
    elif compute_engine.startswith('modin'):
        # NOTE: This section is mostly here for historical reasons.
        # Modin does not appear to support complex shuffle operations,
        # which Dask does.
        if compute_engine.startswith('modin_dask_'):
            import modin.config as modin_cfg
            modin_cfg.Engine.put("dask") # Use dask instead of ray (which is the default)

            if compute_engine == 'modin_dask_distributed':
                cluster, client = start_dask_distributed_over_slurm(num_workers=num_workers, memory_per_worker=memory_per_worker, threads_per_worker=threads_per_worker, **kwargs)
            elif compute_engine == 'modin_dask_local':
                cluster, client = start_dask_local(num_workers=num_workers, memory_per_worker=memory_per_worker, threads_per_worker=threads_per_worker, **kwargs)
            else:
                raise ValueError()

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

    return DataFrameOperations(compute_engine, pd, client, num_workers=num_workers, memory_per_worker=memory_per_worker, threads_per_worker=threads_per_worker), pd

@dataclass
class DataFrameOperations:
    compute_engine: str
    pd: types.ModuleType
    client: any
    num_workers: int
    memory_per_worker: str
    threads_per_worker: int

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
            result = self.rebalance(result)

        return result

    def rebalance(self, df):
        if not self.compute_engine.startswith('dask'):
            return df

        # Rebalances a dask dataframe to roughly equally-sized partitions that do not
        # exceed a threshold.

        df = self.persist(df)
        # https://github.com/dask/dask/blob/91dd42529b9ecd7139926ebadbf56a8f6150991f/dask/dataframe/core.py#L8031
        mem_usages = df.map_partitions(_total_mem_usage, deep=True).compute()
        too_few = len([m for m in mem_usages if m > 0]) < self.num_workers / 5
        # We like to have smaller partitions if running locally, where shuffle is cheaper and spilling is more common.
        # Small partitions mean that much more spilling can occur, because more memory is "managed."
        many_multiplier = 50 if self.compute_engine == 'dask_local' else 10
        too_many = len([m for m in mem_usages if m > 0]) > self.num_workers * many_multiplier
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
        return min(max(total_mem // self.num_workers, 100 * 1_000 * 1_000), self._max_partition_size()) # Don't make smaller than 100MB

    def _max_partition_size(self):
        # Too large makes shuffling impossible, in addition to causing other problems like lots of
        # unmanaged memory.
        if self.memory_per_worker.endswith('GB'):
            memory_per_worker_b = int(self.memory_per_worker.replace('GB', '')) * 1_000 * 1_000 * 1_000
        elif self.memory_per_worker.endswith('MB'):
            memory_per_worker_b = int(self.memory_per_worker.replace('MB', '')) * 1_000 * 1_000
        elif self.memory_per_worker.endswith('KB'):
            memory_per_worker_b = int(self.memory_per_worker.replace('KB', '')) * 1_000
        else:
            raise ValueError()

        # We like to have smaller partitions if running locally, where shuffle is cheaper and spilling is more common.
        # Small partitions mean that much more spilling can occur, because more memory is "managed."
        max_memory_fraction = 20 if self.compute_engine == 'dask_local' else 4
        return memory_per_worker_b // (max_memory_fraction * self.threads_per_worker)

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

        return self.pd.read_parquet(*args, **kwargs).pipe(self.ensure_large_string_capacity).pipe(self.rebalance)

    def to_parquet(self, df, path, *args, wait=False, **kwargs):
        # Dask doesn't overwrite if it is trying to write a directory and there is a file with
        # the same name, even with overwrite=True
        # Pandas won't overwrite if it is trying to write a file and there is a directory with
        # the same name
        utils.remove_path(path)
        result = df.to_parquet(path, *args, **kwargs)

        if wait and self.compute_engine.startswith('dask'):
            import distributed
            distributed.wait(result)

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
    # Add is not defined for large_strings!
    result = ''
    for string in strings:
        if hasattr(string, "astype"):
            result += string.fillna("").astype(str)
        else:
            result += str(string)

    if compute_engine.startswith('dask'):
        return result.astype('large_string[pyarrow]')
    else:
        return result

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

def start_spark_cluster(
    local: bool,
    cpus_master: int,
    memory_master: int,
    num_workers: int,
    checkpoint_directory: str | pathlib.Path,
    local_directory: str | pathlib.Path = "/tmp/spark",
    master_walltime: str = None,
    cpus_per_worker: int = 1,
    worker_walltime: str = None,
    memory_per_worker: str = "10GB",
    worker_memory_overhead_mb: int = 500,
    log_directory: str | pathlib.Path = None,
    scheduler: Literal["slurm", "htcondor", "lsf", "moab", "oar", "pbs", "sge"] = "slurm",
    **extra_scheduler_kwargs,
):
    if local:
        return f"local[{num_workers}]"
    
    memory_master = _convert_to_mb(memory_master)
    memory_per_worker = _convert_to_mb(memory_per_worker)

    assert scheduler == "slurm", "Distributed Spark can currently only be run on Slurm"
    assert master_walltime is not None and worker_walltime is not None

    if log_directory is None:
        log_directory = os.getcwd()

    log_path = f"{log_directory}/spark-master-%j.out"
    sbatch_log_part = f"--output {log_path} --error {log_path}"
    array_job_log_path = f"{log_directory}/spark-worker-%A_%a.out"
    array_job_sbatch_log_part = f"--output {array_job_log_path} --error {array_job_log_path}"

    extra_kwargs_part = ' '.join([f"--{k} '{v}'" for k, v in extra_scheduler_kwargs.items()])        

    code_dir = os.path.abspath(os.path.dirname(__file__))

    print("Starting Spark master")
    spark_start_master_output = os.popen(
        f"sbatch {sbatch_log_part} "
        f"--cpus-per-task {cpus_master} --mem {memory_master} "
        f"--time {master_walltime} {extra_kwargs_part} {code_dir}/start_spark_master.sh"
    ).read()
    spark_master_job_id = re.match('Submitted batch job (\d+)', spark_start_master_output)[1]

    while True:
        try:
            with open(log_path.replace('%j', spark_master_job_id),'r') as file:
                logs = file.read()
            
            starting_lines = [l for l in logs.split('\n') if 'Starting Spark master at' in l]
            webui_starting_lines = [l for l in logs.split('\n') if 'Bound MasterWebUI to' in l]
            if len(starting_lines) > 0 and len(webui_starting_lines) > 0:
                break
        except FileNotFoundError:
            pass
    
        time.sleep(5)        

    spark_master_url = re.match('.*Starting Spark master at (spark:.+)$', starting_lines[0])[1]
    spark_master_webui_url = re.match('.*Bound MasterWebUI to .*, and started at (http:.+)$', webui_starting_lines[0])[1]
    print(f'Got Spark master URL: {spark_master_url}')
    print(f'WebUI running at: {spark_master_webui_url}')

    spark_worker_job_ids = []

    def start_spark_workers(n_workers):
        spark_start_workers_output = os.popen(
            f"sbatch {array_job_sbatch_log_part} "
            f"--array=1-{n_workers} --cpus-per-task {cpus_per_worker} --mem {memory_per_worker + worker_memory_overhead_mb} "
            f"--time {worker_walltime} {extra_kwargs_part} {code_dir}/start_spark_workers.sh {spark_master_url} {local_directory}"
        ).read()
        job_id = re.match('Submitted batch job (\d+)', spark_start_workers_output)[1]
        spark_worker_job_ids.append(job_id)

        while True:
            logs = ''
            for p in glob.glob(array_job_log_path.replace('%A', job_id).replace('%a', '*')):
                try:
                    with open(p,'r') as file:
                        logs += file.read()
                except FileNotFoundError:
                    continue
            
            starting_lines = [l for l in logs.split('\n') if 'Starting Spark worker' in l]
            if len(starting_lines) == n_workers:
                print('\n'.join([l for l in logs.split('\n') if 'Starting Spark worker' in l or 'Bound WorkerWebUI' in l]))
                break
            
            time.sleep(5)

    print('Starting Spark workers')
    start_spark_workers(num_workers)

    def maintain_spark_worker_count():
        while True:
            alive_workers = requests.get(f'{spark_master_webui_url}/json/').json()['aliveworkers']
            if alive_workers < num_workers:
                num_to_start = num_workers - alive_workers
                print(f"Starting {num_to_start} more Spark worker(s)")
                start_spark_workers(num_to_start)
            time.sleep(20)


    maintain_task = asyncio.get_event_loop().run_in_executor(None, maintain_spark_worker_count)

    def teardown():
        maintain_task.cancel()

        for job_id in spark_worker_job_ids:
            os.popen(f"scancel {job_id}").read()

        os.popen(f"scancel {spark_master_job_id}").read()

    # TODO: This cleanup doesn't seem to always work
    atexit.register(teardown)

    # https://moj-analytical-services.github.io/splink/demos/examples/spark/deduplicate_1k_synthetic.html
    from splink.spark.jar_location import similarity_jar_location

    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql import types

    conf = SparkConf()
    conf.setMaster(spark_master_url)
    conf.set("spark.driver.memory", f"{max(memory_master - 1_000, 5_000)}m")
    conf.set("spark.executor.instances", num_workers)
    conf.set("spark.executor.cores", 1)
    conf.set("spark.executor.memory", f"{memory_per_worker}m")

    conf.set("spark.sql.shuffle.partitions", num_workers * 5)
    conf.set("spark.default.parallelism", num_workers * 5)

    # Add custom similarity functions, which are bundled with Splink
    # documented here: https://github.com/moj-analytical-services/splink_scalaudfs
    path = similarity_jar_location()
    conf.set("spark.jars", path)

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    spark.sparkContext.setCheckpointDir(checkpoint_directory)

    return spark, teardown

def _convert_to_mb(memory_string):
    if memory_string.endswith('G') or memory_string.endswith('GB'):
        memory_string = int(re.search(r'(\d+)GB?', memory_string).group(1)) * 1_000
    elif memory_string.endswith('M') or memory_string.endswith('MB'):
        memory_string = int(re.search(r'(\d+)MB?', memory_string).group(1))
    else:
        raise ValueError()
    return memory_string
