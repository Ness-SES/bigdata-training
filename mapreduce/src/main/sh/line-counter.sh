#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

OUTPUT_PATH=/user/ubuntu/training/john

hadoop fs -rm -r $OUTPUT_PATH
hadoop jar map-reduce-1.0-SNAPSHOT.jar com.ness.bigdata.training.mapreduce.linecounter.LineCounterJob /user/ubuntu/word/smallText.txt $OUTPUT_PATH