package com.ness.bigdata.training.spark.pupmed

import javax.xml.parsers.SAXParserFactory

import org.apache.commons.lang3.Validate
import org.apache.commons.lang3.time.DateUtils.parseDate
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}
import scala.util.Try
import scala.xml.{Node, XML}

/**
  * Spark application that parses the Pubmed dataset and extracts a couple of fields from it, saving it finally into
  * Parquet format.
  *
  * @author radu.almasan@ness.com
  */
object XmlToParquet {

  /** Accepted date patterns. */
  val AcceptedDatePatterns = Array("yyyy-MM-dd", "yyyy-MM", "yyyy")

  /**
    * Main entry point of the application.
    *
    * @param args appliciation arguments, expected as an array with 2 elements, first one being the input with the files
    *             containing the paths to the Pubmed articles, and the second one being the non-existing output folder
    */
  def main(args: Array[String]): Unit = {
    Validate.isTrue(args.length == 2, "Expected 2 argument - input path to folder containing parts with file paths to process, and output path")

    val conf = new SparkConf().setAppName("Testing Spark")
    val sc = new SparkContext(conf)

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    if (fileSystem.exists(new Path(args(1)))) {
      throw new IllegalArgumentException("Output path exists")
    }

    val rows = sc.textFile(args(0))
    val filesBySize: RDD[Array[String]] = rows.map(_.split("\\s+"))

    val articles = filesBySize
      .map(_ (1))
      .map(new Path(_))
      .map(getRootElement(_))
      .flatMap(article => {
        val articleData: immutable.Map[String, String] = getArticleData(article)
        val authorsData: immutable.Set[immutable.Map[String, String]] = getAuthorsData(article)

        for (authorData <- authorsData)
          yield newArticle(articleData, authorData)
      })

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val articlesDf = articles.toDF()
    articlesDf.write.parquet(args(1))
  }

  /**
    * Get the root XML element from the given path.
    *
    * @param path the file path of a Pubmed article
    * @return the root XML element
    */
  def getRootElement(path: Path) = {
    val fileSystem = FileSystem.get(new Configuration())
    val fsDataInputStream: FSDataInputStream = fileSystem.open(path)
    val factory = SAXParserFactory.newInstance()
    factory.setValidating(false)
    factory.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false)
    factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)

    val article = XML.loadXML(xml.Source.fromInputStream(fsDataInputStream), factory.newSAXParser())
    Try(fsDataInputStream.close())

    article
  }

  /**
    * Create a new [[Article]] from the given inputs.
    *
    * @param articleData data about the article
    * @param authorData  data about the author
    * @return the new [[Article]]
    */
  def newArticle(articleData: Map[String, String], authorData: Map[String, String]) = {
    Article(
      articleData get "nlm-ta",
      articleData get "iso-abbrev",
      articleData get "journal-title",
      articleData get "ppub",
      articleData get "epub",
      articleData get "publisher-name",
      articleData get "publisher-loc",
      articleData get "pmid" map (_.toInt),
      articleData get "pmc" map (_.toInt),
      articleData get "article-title",
      articleData get "author-surname",
      articleData get "author-given-names",
      articleData get "author-email",
      articleData.get("pub-date-epub")
        .map(parseDate(_, AcceptedDatePatterns: _*))
        .map(_.getTime)
        .map(new java.sql.Date(_)),
      articleData.get("pub-date-pmc-release")
        .map(parseDate(_, AcceptedDatePatterns: _*))
        .map(_.getTime)
        .map(new java.sql.Date(_)),
      articleData.get("pub-date-ppub")
        .map(parseDate(_, AcceptedDatePatterns: _*))
        .map(_.getTime)
        .map(new java.sql.Date(_)),
      articleData get "volume" map (_.toInt),
      articleData get "issue" map (_.toInt),
      articleData get "fpage" map (_.toInt),
      articleData get "lpage" map (_.toInt))
  }

  /**
    * Get data about authors.
    *
    * @param article the root article node
    * @return data about the authors
    */
  def getAuthorsData(article: Node): immutable.Set[immutable.Map[String, String]] = {
    val authorsData = mutable.Set[immutable.Map[String, String]]()

    val articleMeta = article \ "front" \ "article-meta"

    (articleMeta \ "contrib-group" \ "contrib")
      .filter(_.attribute("contrib-type").exists(_.text == "author"))
      .foreach(contrib => {
        val data = mutable.Map[String, String]()

        Seq("surname", "given-names").foreach(name =>
          (contrib \ "name" \ name).headOption.map(_.text).foreach(value => data += (s"author-$name" -> value)))

        (contrib \ "address" \ "email").headOption.map(_.text).foreach(email => data += ("email" -> email))

        authorsData += (immutable.Map() ++ data)
      })

    immutable.Set() ++ authorsData
  }

  /**
    * Get the article data from the XML.
    *
    * @param article the article XML node
    * @return an immutable map with the article data
    */
  def getArticleData(article: Node): immutable.Map[String, String] = {
    val articleData = collection.mutable.Map[String, String]()

    val journalMeta = article \ "front" \ "journal-meta"

    (journalMeta \ "journal-id")
      .map(journalId => (journalId.attribute("journal-id-type").map(_.text).filter(Seq("nlm-ta", "iso-abbrev").contains(_)), journalId.text))
      .filter(_._1.isDefined)
      .map(p => (p._1.get, p._2))
      .foreach(articleData += _)

    (journalMeta \ "journal-title-group" \ "journal-title").headOption.foreach(x => articleData += ("journal-title" -> x.text))

    (journalMeta \ "issn")
      .map(issn => (issn attribute "pub-type" map (_.text) filter (Seq("ppub", "epub") contains _), issn.text))
      .filter(_._1.isDefined)
      .map(p => (p._1.get, p._2))
      .foreach(articleData += _)

    val publisher = journalMeta \ "publisher"
    articleData += ("publisher-name" -> (publisher \ "publisher-name").text)
    articleData += ("publisher-loc" -> (publisher \ "publisher-loc").text)

    val articleMeta = article \ "front" \ "article-meta"

    (articleMeta \ "article-id")
      .map(articleId => (articleId attribute "pub-id-type" map (_.text) filter (Seq("pmid", "pmc") contains _), articleId.text))
      .filter(_._1.isDefined)
      .map(p => (p._1.get.trim, p._2))
      .filter(p => Try(p._2.toInt).isSuccess)
      .foreach(articleData += _)

    (articleMeta \ "title-group" \ "article-title").headOption.map(_.text).foreach(title => articleData += ("article-title" -> title))

    (articleMeta \ "pub-date")
      .map(pubDate => (pubDate, pubDate.attribute("pub-type").map(_.text).filter(Seq("epub", "pmc-release", "ppub").contains(_))))
      .filter(p => p._2.isDefined)
      .map(p => (p._2.get, Seq("year", "month", "day").map(p._1 \ _).flatMap(_.headOption).map(_.text).mkString("-")))
      .foreach(articleData += _)

    Seq("volume", "issue", "fpage", "lpage").foreach(attr =>
      (articleMeta \ attr).headOption.map(_.text).filter(n => Try(n.toInt).isSuccess).foreach(value => articleData += (attr -> value)))

    immutable.Map() ++ articleData
  }
}
