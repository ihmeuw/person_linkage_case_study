#!/bin/env python

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

args = parser.parse_args()

host = socket.getfqdn()

if args.conda_path not in ("", "None"):
    conda_activate_part = f"""
    source {Path(args.conda_path).parent.parent / "etc/profile.d/conda.sh"}
    conda activate {args.conda_prefix}
    """
else:
    conda_activate_part = ""

if args.venv_path not in ("", "None"):
    venv_activate_part = f"source {args.venv_path}/bin/activate"
else:
    venv_activate_part = ""

print(args.singularity_args)

script = f"""
{conda_activate_part}
{venv_activate_part}
"/opt/spark/bin/spark-class" org.apache.spark.deploy.master.Master --host "{host}" --port 28508 --webui-port 28509
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
