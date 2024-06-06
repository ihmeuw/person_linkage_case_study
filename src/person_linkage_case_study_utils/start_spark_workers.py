#!/bin/env python

import os
import socket
import subprocess
from argparse import ArgumentParser
from pathlib import Path

parser = ArgumentParser()
parser.add_argument("singularity_path")
parser.add_argument("singularity_image")
parser.add_argument("singularity_args")
parser.add_argument("conda_path")
parser.add_argument("conda_prefix")
parser.add_argument("venv_path")
parser.add_argument("master_url")
parser.add_argument("local_dir")

args = parser.parse_args()

host = socket.getfqdn()

if args.conda_path != "":
    conda_activate_part = f"""
    source {Path(args.conda_path).parent.parent / "etc/profile.d/conda.sh"}
    conda activate {args.conda_prefix}
    """
else:
    conda_activate_part = ""

if args.venv_path != "":
    venv_activate_part = f"source {args.venv_path}/bin/activate"
else:
    venv_activate_part = ""

memory = os.environ["SLURM_MEM_PER_NODE"]

script = f"""
{conda_activate_part}
{venv_activate_part}
"/opt/spark/bin/spark-class" org.apache.spark.deploy.worker.Worker --cores 1 --memory {memory}M --webui-port 28510 --work-dir {args.local_dir} {args.master_url}
"""

subprocess.run(
    [
        args.singularity_path,
        "exec",
        *args.singularity_args.split(" "),
        args.singularity_image,
        "/bin/bash",
        "-c",
        script,
    ]
)
