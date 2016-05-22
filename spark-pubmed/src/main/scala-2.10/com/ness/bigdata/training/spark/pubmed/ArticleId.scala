package com.ness.bigdata.training.spark.pubmed

import java.sql.Date

/**
 * @author adrian.buciuman@ness.com
 */
case class ArticleId(path: String,
                   uuid: String = java.util.UUID.randomUUID().toString()) {
}