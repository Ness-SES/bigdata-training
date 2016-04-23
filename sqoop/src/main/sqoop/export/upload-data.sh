#!/usr/bin/env bash

sudo -u hdfs \
hdfs dfs \
    -copyFromLocal ./data.txt sqoop_export/
