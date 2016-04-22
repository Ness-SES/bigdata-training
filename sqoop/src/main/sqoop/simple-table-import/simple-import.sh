#!/usr/bin/env bash

sudo -u hdfs sqoop import —connect jdbc:myqsl://localhost/test —username root —table mytable —target-dir sqoopimport1
