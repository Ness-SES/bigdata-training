#!/usr/bin/env bash

hostname=$(hostname)
database=
username=
password=

sudo -u hdfs \
sqoop import \
    --connect jdbc:postgresql://$hostname:5432/${database} \
    --username ${username} \
    --password ${password} \
    --table import_table \
    --split-by id \
    --target-dir /tmp/sqoop_import
