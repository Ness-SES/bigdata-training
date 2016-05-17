import org.apache.spark.sql.Row

val row = Row("a", "b", "c", 1, 2, null)

Seq(row).foreach{
  case Row(s1: String, s2: String, s3: String, n1: Int, n2: Option[Int], n3) => println("typed row")
}
