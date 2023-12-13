# Wrapper scripts to interact with Slurm from within a Singularity container

Slurm needs certain users to exist, so e.g. `sbatch` can't be run directly inside a Singularity container.
See [this thread](https://groups.google.com/a/lbl.gov/g/singularity/c/syLcsIWWzdo) and
[this thread](https://groups.google.com/a/lbl.gov/g/singularity/c/cZXAL05-pc4/m/e4NX6D2MAgAJ) for more
background on this.

The simplest solution, which I have adopted here, is to use SSH to get *back* to the host whenever we
need to execute a Slurm command.
I found out about this approach in [this Gist](https://gist.github.com/willirath/2176a9fa792577b269cb393995f43dda).
It needed some adapting to better create a login shell, and to account for the binding of /tmp.

When `ssh` is not available, there are more advanced/cool ways to achieve the same thing,
see [this GitHub repo](https://github.com/ExaESM-WP4/Batch-scheduler-Singularity-bindings) for an example.
However, I stuck with the simpler solution since our cluster doesn't restrict `ssh`.