#!/usr/bin/env bash

hostname=$(hostname)
database=
username=
password=

sudo -u hdfs \
sqoop export \
    --connect jdbc:postgresql://$hostname:5432/${database} \
    --username ${username} \
    --password ${password} \
    --table export_table \
    --export-dir /tmp/sqoop_export
