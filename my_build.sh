#!/bin/bash

CONDA_BUILD_DIR=/tmp/conda
CONDA_INSTALL_DIR=/tmp/condaruntime
PANDAS_DIR=/home/ubuntu/pandas
THRIFT='thrift==0.13.0'
CYTHON='Cython==0.29.15'
NUMPY='numpy==1.18.1'

RUNTIME_S3_BUCKET="jiffy-error"
RUNTIME_S3_META="pywren.runtime/pywren_runtime-3.6-default_pandas.meta.json"
RUNTIME_S3_TAR="pywren.runtime/pywren_runtime-3.6-default_pandas.tar.gz"

TAR_S3_URL="s3://$RUNTIME_S3_BUCKET/$RUNTIME_S3_TAR"
META_S3_URL="s3://$RUNTIME_S3_BUCKET/$RUNTIME_S3_META"

rm -rf $CONDA_BUILD_DIR
mkdir -p $CONDA_BUILD_DIR
cd $CONDA_BUILD_DIR
aws s3 cp s3://pywren-runtimes-public-us-west-2/dad11e-pywren.runtimes/default_3.6.tar.gz.0019 .
aws s3 cp s3://pywren-runtimes-public-us-west-2/pywren.runtimes/default_3.6.meta.json .

# edit urls
cat ./default_3.6.meta.json | jq -c ".urls = [\"${TAR_S3_URL}\"]" > runtime.meta.json


rm -rf $CONDA_INSTALL_DIR
tar xf default_3.6.tar.gz.0019 -C /tmp

cd $CONDA_INSTALL_DIR
# install thrift
$CONDA_INSTALL_DIR/bin/python3 -m pip install $THRIFT --ignore-installed --prefix=$CONDA_INSTALL_DIR
# install cython for building pandas
$CONDA_INSTALL_DIR/bin/python3 -m pip install $CYTHON --ignore-installed --prefix=$CONDA_INSTALL_DIR
# remove old version numpy
rm -rf $CONDA_INSTALL_DIR/lib/python3.6/site-packages/numpy/
rm -rf $CONDA_INSTALL_DIR/lib/python3.6/site-packages/numpy-1.13.1-py3.6.egg-info
$CONDA_INSTALL_DIR/bin/python3 -m pip install $NUMPY --ignore-installed --prefix=$CONDA_INSTALL_DIR
# install pandas
$CONDA_INSTALL_DIR/bin/python3 -m pip install $PANDAS_DIR --ignore-installed --prefix=$CONDA_INSTALL_DIR

# shrink
python2 /home/ubuntu/runtimes/shrinkconda.py $CONDA_INSTALL_DIR

cd /tmp
rm -f condaruntime.tar.gz
tar czf condaruntime.tar.gz condaruntime


aws s3 cp $CONDA_BUILD_DIR/runtime.meta.json $META_S3_URL

aws s3 cp condaruntime.tar.gz $TAR_S3_URL

echo "Done"
echo "runtime:"
echo "   s3_bucket: $RUNTIME_S3_BUCKET"
echo "   s3_key: $RUNTIME_S3_META"
