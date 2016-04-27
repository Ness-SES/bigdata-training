package com.ness.bigdata.training.spark.pubmed

import org.apache.commons.lang3.Validate
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark application to change file paths with old `rotimlxdv` domain name to the new `rotsrlxdv` domain name.
  *
  * @author radu.almasan@ness.com
  */
object TimToTsr {

  /** Spark application name. */
  val APP_NAME = "tim-to-tsr"

  /**
    * Main entry point of the application.
    *
    * @param args appliciation arguments, expected as an array with 2 elements, first one being the input with the files
    *             containing the paths to the Pubmed articles, and the second one being the non-existing output folder
    */
  def main(args: Array[String]) {
    Validate.isTrue(args.length == 2, "Expected 2 arguments: input path to folder containing parts with file paths to process, and path to non existing output folder")

    val conf = new SparkConf().setAppName(APP_NAME)
    val sc = new SparkContext(conf)

    val rows = sc.textFile(args(0))
    val filesBySize = rows.map(_.split("\\s+"))

    filesBySize
      .map(pair => (pair(0).toInt, "rotimlxdv".r.replaceAllIn(pair(1), "rotsrlxdv")))
      //.repartitionAndSortWithinPartitions(new HashPartitioner(4))
      .map { case (key, value) => s"$key $value" }
      .saveAsTextFile(args(1))
  }
}
