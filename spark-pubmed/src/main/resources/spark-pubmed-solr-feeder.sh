#!/usr/bin/env bash

# solrctl instancedir --generate <path>
# solrctl instancedir --create <confif_name> <path>
# solrctl collection --create <name> -s <shards_number> -c <config_name>

sudo -u hdfs \
spark-submit \
        --class com.ness.bigdata.training.spark.pubmed.SolrFeeder \
        --repositories "https://repository.cloudera.com/artifactory/cloudera-repos/" \
        --packages org.apache.solr:solr-solrj:${solrj.version} \
        --master yarn-client \
	--conf "spark.dynamicAllocation.enabled=true" \
	--conf "spark.executor.memory=1800MB" \
	--conf "spark.executor.cores=2" \
	--conf "spark.executor.instances=7" \
        ./${project.build.finalName}.${project.packaging} \
        rotsrlxdv01.ness.com:2181,rotsrlxdv02.ness.com:2181,rotsrlxdv03.ness.com:2181/solr \
        article \
        /wmg/articles_with_body
