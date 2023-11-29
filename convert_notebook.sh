#!/usr/bin/env bash
# coding: utf-8

# Convert a notebook to a Python script
jupyter nbconvert --config ../../nbconvert_no_magic/config.py --to python --template ../../nbconvert_no_magic/template $1.ipynb --stdout |
    # https://stackoverflow.com/a/922538/ (comment)
    grep -v -A1 '^[[:blank:]]*$' |
    grep -v '^--$' > $1.py