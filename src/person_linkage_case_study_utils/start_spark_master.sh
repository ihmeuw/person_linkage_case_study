#!/bin/bash

CONDA_PATH=$1
CONDA_ENV=$2
SINGULARITY_IMG=docker://apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63

SPARK_MASTER_PORT=28508
SPARK_MASTER_WEBUI_PORT=28509

SPARK_MASTER_HOST=$(hostname -f)

mkdir -p /tmp/singularity_spark_$USER
/opt/singularity/bin/singularity exec -B /mnt:/mnt,/tmp/singularity_spark_$USER:/tmp \
    $SINGULARITY_IMG \
    $CONDA_PATH run --no-capture-output -n $CONDA_ENV \
    "/opt/spark/bin/spark-class" org.apache.spark.deploy.master.Master \
    --host "$SPARK_MASTER_HOST" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_MASTER_WEBUI_PORT"

