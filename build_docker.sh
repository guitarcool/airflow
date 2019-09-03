#!/bin/bash

# pip install virtualenv
# yum install python36 python36-devel

set -e

#env_folder=airflow-builder-env

#rm -rf $env_folder

#virtualenv --no-site-packages --python=python3.6 $env_folder
#source $env_folder/bin/activate
#pip install .
#deactivate

#docker rmi airflow-base
echo "building docker..."
docker build -t airflow-base .

echo "exporting docker..."
docker save airflow-base | gzip > /opt/airflow-base.tar.gz
chmod 666 /opt/airflow-base.tar.gz
