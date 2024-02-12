#!/bin/bash
#start_spark_slurm.sh

# NOTE: This script has a lot in it, and most of it is probably unnecessary.
# It is a heavily modified version of https://serverfault.com/a/776688

CONDA_PATH=/mnt/share/homes/zmbc/mambaforge/condabin/mamba # must be accessible within container
CONDA_ENV=person_linkage_case_study
SINGULARITY_IMG=docker://apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63

SPARK_ROOT=/opt/spark # within the container
SPARK_MASTER_PORT=28508
SPARK_MASTER_WEBUI_PORT=28509
SPARK_WORKER_WEBUI_PORT=28510
MASTER_URL=$1
SPARK_WORKER_MEMORY_OVERHEAD="${2:-5000}"

mkdir -p /tmp/singularity_spark_$USER/spark_work
# cores=1 so it doesn't take on multiple tasks at a time?!
/opt/singularity/bin/singularity exec -B /mnt:/mnt,/tmp/singularity_spark_$USER:/tmp \
    $SINGULARITY_IMG \
    $CONDA_PATH run --no-capture-output -n $CONDA_ENV \
    "$SPARK_ROOT/bin/spark-class" org.apache.spark.deploy.worker.Worker \
    --cores 1 --memory "$SLURM_MEM_PER_NODE"M \
    --webui-port "$SPARK_WORKER_WEBUI_PORT" \
    --work-dir /tmp/spark_work \
    $MASTER_URL