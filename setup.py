#!/usr/bin/env python
import os

from setuptools import setup, find_packages

if __name__ == "__main__":

    base_dir = os.path.dirname(__file__)
    src_dir = os.path.join(base_dir, "src")

    about = {}
    with open(os.path.join(src_dir, "person_linkage_case_study_utils", "__about__.py")) as f:
        exec(f.read(), about)

    with open(os.path.join(base_dir, "README.md")) as f:
        long_description = f.read()

    install_requirements = [
        # Core libraries
        "pandas",
        "pyarrow",
        "numpy",
        "matplotlib",
        "pseudopeople",
        "splink",
        "jellyfish",
        # Workflow management
        "snakemake",
        "papermill",
        # Pins
        "pulp<2.8",  # Needed for snakemake, see https://github.com/snakemake/snakemake/issues/2607#issuecomment-1948732242
    ]
    dask_requirements = [
        "pseudopeople[dask]",
        "dask_jobqueue",
        "bokeh!=3.0.*,>=2.4.2",  # needed for dask dashboard
    ]
    spark_requirements = [
        "pyspark==3.4.1",  # NOTE: I have no idea why, but pyspark 3.5.0 (with the correct version of Spark) would hang forever on the first stage
    ]
    dev_requirements = (
        [
            "jupyterlab",
            "nbdime",
            "black[jupyter]",
        ]
        + dask_requirements
        + spark_requirements
    )

    setup(
        name=about["__title__"],
        version=about["__version__"],
        description=about["__summary__"],
        long_description=long_description,
        license=about["__license__"],
        url=about["__uri__"],
        author=about["__author__"],
        author_email=about["__email__"],
        package_dir={"": "src"},
        packages=find_packages(where="src"),
        include_package_data=True,
        install_requires=install_requirements,
        extras_require={
            "dask": dask_requirements,
            "spark": spark_requirements,
            "dev": dev_requirements,
        },
        zip_safe=False,
    )
