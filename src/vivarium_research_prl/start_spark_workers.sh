#!/bin/bash

CONDA_PATH=$1
CONDA_ENV=$2
SINGULARITY_IMG=docker://apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63

SPARK_WORKER_WEBUI_PORT=28510
MASTER_URL=$3
LOCAL_DIR=$4

mkdir -p /tmp/singularity_spark_$USER/spark_work
# cores=1 so it doesn't take on multiple tasks at a time?!
/opt/singularity/bin/singularity exec -B /mnt:/mnt,/tmp/singularity_spark_$USER:/tmp \
    $SINGULARITY_IMG \
    $CONDA_PATH run --no-capture-output -n $CONDA_ENV \
    "/opt/spark/bin/spark-class" org.apache.spark.deploy.worker.Worker \
    --cores 1 --memory "$SLURM_MEM_PER_NODE"M \
    --webui-port "$SPARK_WORKER_WEBUI_PORT" \
    --work-dir $LOCAL_DIR \
    $MASTER_URL