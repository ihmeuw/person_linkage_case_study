#!/bin/bash
#start_spark_slurm.sh

#SBATCH --nodes=3
#  ntasks per node MUST be one, because multiple workers per node doesn't
#  work well with slurm + spark in this script (they would need increasing
#  ports among other things)
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=5
#SBATCH --mem=8G
#SBATCH --time=10-00:00:00

# NOTE: This script has a lot in it, and most of it is probably unnecessary.
# It is a heavily modified version of https://serverfault.com/a/776688

CONDA_PATH=/mnt/share/homes/zmbc/mambaforge/condabin/mamba # must be accessible within container
CONDA_ENV=person_linkage_case_study
SINGULARITY_IMG=docker://apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63

SPARK_ROOT=/opt/spark # within the container
SPARK_MASTER_PORT=28508
SPARK_MASTER_WEBUI_PORT=28509
SPARK_WORKER_WEBUI_PORT=28510
MASTER_URL=$2

# This section will be run when started by sbatch
if [ "$1" != 'multi_job' ]; then
    this=$0
    # I experienced problems with some nodes not finding the script:
    #   slurmstepd: execve(): /var/spool/slurm/job123/slurm_script:
    #   No such file or directory
    # that's why this script is being copied to a shared location to which
    # all nodes have access:
    mkdir -p $HOME/.spark_temp
    script=$HOME/.spark_temp/${SLURM_JOBID}_$( basename -- "$0" )
    cp "$this" "$script"

    /opt/slurm/bin/srun $script 'multi_job' $@
else
    mkdir -p /tmp/singularity_spark_$USER/spark_work
    /opt/singularity/bin/singularity exec -B /mnt:/mnt,/tmp/singularity_spark_$USER:/tmp \
        $SINGULARITY_IMG \
        $CONDA_PATH run --no-capture-output -n $CONDA_ENV \
        "$SPARK_ROOT/bin/spark-class" org.apache.spark.deploy.worker.Worker \
        --cores "$SLURM_CPUS_PER_TASK" --memory "$SLURM_MEM_PER_NODE"M \
        --webui-port "$SPARK_WORKER_WEBUI_PORT" \
        --work-dir /tmp/spark_work \
        $MASTER_URL
fi

