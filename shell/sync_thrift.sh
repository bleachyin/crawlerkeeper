#!/bin/bash
THRIFT_FILE_PATH="../conf/service.thrift"
THRIFT_SERVICE_PATH="../service"
if [ ! -d "$THRIFT_SERVICE_PATH" ];then
mkdir $THRIFT_SERVICE_PATH
fi
thrift -out $THRIFT_SERVICE_PATH -gen py $THRIFT_FILE_PATH

