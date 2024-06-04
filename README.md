# Person linkage case study

This case study emulates the methods the US Census Bureau
uses to link people across multiple data sources,
using open-source software
([Splink](https://moj-analytical-services.github.io/splink/index.html))
and simulated data (from [pseudopeople](https://pseudopeople.readthedocs.io/en/latest/)).
It is based on public descriptions of the [Person Identification Validation System](https://www.census.gov/about/adrm/linkage/projects/pvs.html),
which is one of the Census Bureau's primary person linkage pipelines.
The case study runs at multiple scales, including at full-USA scale --
hundreds of billions of record comparisons.
This presents a realistic test case for assessing the
computational performance and accuracy of record linkage methods improvements.
The case study also serves as a concrete example of the sorts of methods
the Census Bureau uses to link files, and may be of interest to those who would
like to understand those methods better.

## Quickstart

You can run the case study at small scale on your laptop in just a few minutes.
First, install conda if you don't have it already --
we recommend [Miniforge](https://github.com/conda-forge/miniforge).

Clone this repository to your computer and enter this directory:

```console
$ git clone https://github.com/ihmeuw/person_linkage_case_study
$ cd person_linkage_case_study
```

Install dependencies with conda and pip:

```console
$ conda create --name person_linkage_case_study --file conda.lock.txt
$ conda activate person_linkage_case_study
(person_linkage_case_study) $ pip install -r pip.lock.txt
(person_linkage_case_study) $ pip install -e .
```

If the `conda.lock.txt` line doesn't work for some reason, you can roughly recreate
the environment necessary with `conda create --name person_linkage_case_study python=3.10`.
If the `pip.lock.txt` line doesn't work for some reason, it can be skipped to
approximate the environment.

Now, you can run the case study like so:

```console
(person_linkage_case_study) $ snakemake --forceall
```

This will run the case study on about 10,000 rows of sample data.

The `diagnostics` folder will be updated with diagnostics about the linkage models
you ran (in the `small_sample` subfolders).
Specifically, take a look at
[`diagnostics/executed_notebooks/small_sample/04_calculate_ground_truth_accuracy.ipynb`](./diagnostics/executed_notebooks/small_sample/04_calculate_ground_truth_accuracy.ipynb)
for information on how accurate the linkage was.
In addition, [`benchmarks/benchmark-small_sample.txt`](./benchmarks/benchmark-small_sample.txt)
will be updated with computational performance information about the core linking part
of the case study.
These two files are the most important outputs for evaluating new methods:
can methods improvements improve the accuracy, or improve the runtime/resources without
substantially decreasing the accuracy?

**Note: In actual use, these questions should be investigated at full scale; this case study
does not attempt to realistically represent how the Census Bureau would link smaller files.
The smaller scales are included for experimentation, getting started, and testing.**

## Dask and Spark

The small-scale run in the previous section did not use any parallel or distributed processing,
which will be needed in order to scale up.
You can set up the technologies needed and test them on the small-scale data.

Create a file called `overrides.yaml` in the `config/` directory. Give it the following contents:

```yaml
papermill_params:
  small_sample:
    all:
      compute_engine: dask_local
    link_datasets:
      splink_engine: spark
      spark_local: True
```

These overrides say to use Dask and Spark locally (on your computer).
[Dask](https://www.dask.org/) is used by the case study itself and
[Spark](https://spark.apache.org/) is used within Splink.

You will need to install [Singularity](https://docs.sylabs.io/guides/latest/user-guide/) to run this, because Spark cannot
be installed via conda.

Now, run `snakemake --forceall` to re-run the entire case study using these settings.
You will see a lot more output this time, but you should get the same result.

**Note: Both Dask and Spark can spill data to disk. As you run at larger scales,
you will want to make sure you have both enough RAM and enough empty disk space.**

## Distributed Dask

In the previous section, Dask ran entirely on a single computer, which puts limits
on how many resources it can use: as many as you have on one machine.
You can run Dask across many computers in a cluster like this:

```yaml
papermill_params:
  small_sample:
    all:
      compute_engine: dask
      compute_engine_scheduler: slurm
      queue: <your Slurm partition>
      account: <your Slurm account>
      walltime: 1-00:00:00 # 1 day
```

This is powered by [dask_jobqueue](https://jobqueue.dask.org/en/latest/), which supports a number of schedulers besides
Slurm.
You will likely also want to configure the resources requested; see [`config/defaults.yaml`](.config/defaults.yaml)
for examples of this.

## Distributed Spark

The same reasoning goes for Spark as for Dask: you may want to run it across multiple machines
to utilize more compute resources.

Unfortunately, a flexible library like dask_jobqueue doesn't exist for Spark.
Currently, **this will only work on a Slurm cluster**.
Support for more schedulers may be added in the future.
The configuration looks as follows:

```yaml
papermill_params:
  small_sample:
    all:
      queue: <your Slurm partition>
      account: <your Slurm account>
      walltime: 1-00:00:00 # 1 day
    link_datasets:
      splink_engine: spark
      spark_local: False
```

You will now need access to Slurm from inside the Singularity image for Spark.
An example container for this purpose, which works on the IHME cluster, is included in
this repository. To use this example, run

```console
$ singularity build --fakeroot spark_slurm_container/spark.sif spark_slurm_container/Singularity
```

and then add `custom_spark_container_path: spark_slurm_container/spark.sif` to the top
level of your configuration YAML.

## Distributed Dask *and* Spark

You can use any combination of local or distributed Dask or Spark.
For example, to run both distributed, your configuration would look like this:

```yaml
papermill_params:
  small_sample:
    all:
      compute_engine: dask
      compute_engine_scheduler: slurm
      queue: <your Slurm partition>
      account: <your Slurm account>
      walltime: 1-00:00:00 # 1 day
    link_datasets:
      splink_engine: spark
      spark_local: False
```

## Scaling up

There are three scales to choose from:

- `small_sample`, which is the default of about 10,000 simulated people
- `ri`, which uses the simulated state of Rhode Island (about 1 million simulated people)
- `usa`, which uses an entire simulated USA population (about 330 million simulated people)

**Note: as mentioned above, the smaller-scale options are intended for experimentation, setup, and testing.
The USA scale is the only one that attempts to realistically emulate Census Bureau processes.**

To move up from `small_sample`, you'll need access to a larger simulated population.
Instructions to request access can be found [in the pseudopeople documentation](https://pseudopeople.readthedocs.io/en/latest/simulated_populations/index.html).
Once you have downloaded and unzipped your simulated population, you can configure
the case study to use it like so:

```yaml
data_to_use: ri
papermill_params:
  ri:
    generate_pseudopeople_simulated_datasets:
      ri_simulated_population: /path/to/your/simulated/population
```

The default configuration contains defaults adapted to each scale, to request
about the amount of resources you will need.
You can also override these, of course.

## Making methods changes

The primary purpose of the case study is to be used in evaluating changes in linkage methods.
Once you have the case study running, the next step is to tweak something and
see what effect that has on accuracy and/or runtime.
You'll want to make edits only in `03_link_datasets.ipynb`, as this is the part of the
case study that simulates the linkage process; the other notebooks are setup and evaluation.
When you edit the notebook and run `snakemake`, Snakemake should recognize that it only needs
to re-run the linkage and the accuracy assessment, skipping the redundant setup.

## What's next?

We plan to iterate on this person linkage case study over time to improve its realism,
usability, and computational efficiency.
Please feel free to open an issue if you have ideas or need help!