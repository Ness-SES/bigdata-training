#!/usr/bin/env bash

sudo -u hdfs \
sqoop import \
    —connect jdbc:myqsl://localhost/test \
    —username root \
    —table import_table \
    —target-dir sqoopimport1
