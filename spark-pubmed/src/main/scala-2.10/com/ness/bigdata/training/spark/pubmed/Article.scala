package com.ness.bigdata.training.spark.pubmed

import java.sql.Date

/**
  * @author radu.almasan@ness.com
  */
case class Article(nlmTa: Option[String],
                   isoAbbrev: Option[String],
                   journalTitle: Option[String],
                   ppub: Option[String],
                   epub: Option[String],
                   publisherName: Option[String],
                   publisherLoc: Option[String],
                   pmid: Option[Int],
                   pmc: Option[Int],
                   articleTitle: Option[String],
                   authorSurname: Option[String],
                   authorGivenNames: Option[String],
                   authorEmail: Option[String],
                   pubDateEpub: Option[Date],
                   pubDatePmcRelease: Option[Date],
                   pubDatePpub: Option[Date],
                   volume: Option[Int],
                   issue: Option[Int],
                   fpage: Option[Int],
                   lpage: Option[Int],
                   body: Option[String]) {
}
