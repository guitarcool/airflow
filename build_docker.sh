#!/bin/bash

# pip install virtualenv
# yum install python36 python36-devel

set -e

env_folder=airflow-builder-env

rm -rf $env_folder

virtualenv --no-site-packages --python=python3.6 $env_folder
source $env_folder/bin/activate
pip install .
deactivate

docker build --rm -t airflow-base .
