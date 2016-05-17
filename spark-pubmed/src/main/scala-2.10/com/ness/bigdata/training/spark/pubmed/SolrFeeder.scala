package com.ness.bigdata.training.spark.pubmed

import java.util.UUID

import org.apache.commons.lang3.Validate
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._


/**
  * @author radu.almasan@ness.com on 09/05/16.
  */
object SolrFeeder {

  /** Document indexing batch size. */
  val BatchSize: Int = 1000
  /** Number of expected arguments. */
  val ExpectedNumerOfArguments = 3

  /**
    * Entry point of the application.
    *
    * @param args expected 3 arguments - zkHost, collection name, and input path to articles folder
    */
  def main(args: Array[String]) {
    Validate.isTrue(args.length == ExpectedNumerOfArguments, s"Expected $ExpectedNumerOfArguments arguments\n" +
      s" - zkhost (host:port)" +
      s" - Solr collection name" +
      s" - input path to articles folder with parquet files")

    val (zkHost, collection, inputPath) = (args(0), args(1), args(2))

    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val articles = sqlContext.read.parquet(inputPath)

    articles
      .map(row => {
        val document = new SolrInputDocument()
        row.schema.fieldNames.foreach(fieldName => document.addField(fieldName, row.getAs(fieldName)))
        document.setField("id", UUID.randomUUID().toString)
        document
      })

      .foreachPartition(documents => {
        val solrClient = new CloudSolrClient(zkHost)
        solrClient.setDefaultCollection(collection)
        documents.grouped(BatchSize).foreach(batch => {
          solrClient.add(batch.asJava)
          solrClient.commit()
        })
      })
  }
}
