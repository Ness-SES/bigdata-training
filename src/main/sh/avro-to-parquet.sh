#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

INPUT_PATH=/user/ubuntu/adyb/pubmedTest
OUTPUT_PATH=/user/ubuntu/adyb/pubmedRes

hadoop fs -rm -r $OUTPUT_PATH
hadoop jar map-reduce-1.0-SNAPSHOT.jar com.ness.bigdata.training.mapreduce.pubmed.parquet.AVROToParquetEngine $INPUT_PATH $OUTPUT_PATH $1 