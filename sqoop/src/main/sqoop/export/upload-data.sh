#!/usr/bin/env bash

exportFolder="/tmp/sqoop_export/"

sudo -u hdfs hdfs dfs -mkdir -p ${exportFolder}

sudo -u hdfs hdfs dfs -copyFromLocal -f ./data.csv ${exportFolder}
