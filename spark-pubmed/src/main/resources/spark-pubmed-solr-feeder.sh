#!/usr/bin/env bash

sudo -u hdfs \
spark-submit \
        --class com.ness.bigdata.training.spark.pubmed.SolrFeeder \
        --repositories "https://repository.cloudera.com/artifactory/cloudera-repos/" \
        --packages org.apache.solr:solr-solrj:${solrj.version} \
        --master yarn-client \
        ./${project.build.finalName}.${project.packaging} \
        rotsrlxdv01.ness.com:2181,rotsrlxdv02.ness.com:2181,rotsrlxdv03.ness.com:2181/solr \
        article \
        /wmg/articles_with_body
