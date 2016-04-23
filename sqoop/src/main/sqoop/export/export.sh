#!/usr/bin/env bash

sudo -u hdfs \
sqoop export \
    -connect jdbc:myqsl://localhost/test \
    -username root \
    -table export_table \
    -export-dir sqoop_export
