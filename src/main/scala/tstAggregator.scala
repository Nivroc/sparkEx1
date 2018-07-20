
import java.sql.Timestamp
import java.time.temporal.{ChronoUnit, Temporal, TemporalUnit}

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._

object tstAggregator extends App {

  case class RealTableRow(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String)
  case class TableRow(category: String, product: String, userId: Int, time: Int)
  case class SaturatedTableRow(category: String, userId: String, eventTime: Timestamp, sessionId:Int, sessionStart: Timestamp, sessionEnd: Timestamp)
  case class A(value :Map[(String, String), Seq[Timestamp]])
  case class B(value :Seq[SaturatedTableRow])

  object MyAgg extends Aggregator[RealTableRow, A, B] {

    override def zero: A = A(Map.empty[(String, String), Seq[Timestamp]])

    override def reduce(b: A, a: RealTableRow): A =
      A(b.value.updated((a.category, a.userId), b.value.getOrElse((a.category, a.userId), Nil) :+ a.eventTime))

    override def merge(b1: A, b2: A): A =
      A((b1.value.keySet ++ b2.value.keySet).map(key => (key, b1.value.getOrElse(key, Nil) ++ b2.value.getOrElse(key, Nil))).toMap)

    override def finish(reduction: A): B = {
      val st1 = reduction.value.map(kv => kv._1 -> kv._2.sortWith((x, y) => x.before(y)).foldLeft(Seq.empty[Seq[Timestamp]])((s, e) =>
        if (s.nonEmpty && s.last.last.toLocalDateTime.minus(5, ChronoUnit.MINUTES).isBefore(e.toLocalDateTime)) s.init :+ (s.last :+ e) else s :+ Seq(e)))
      val globalIds = st1.values.toSeq.flatten.zipWithIndex.toMap
      B(st1.foldLeft(Seq.empty[SaturatedTableRow])((sq, kv) =>
        sq ++ kv._2.flatMap(sq => sq.map(time => SaturatedTableRow(kv._1._1, kv._1._2, time, globalIds(sq), sq.head, sq.last)))))
    }

    override def bufferEncoder: Encoder[A] = Encoders.product[A]

    override def outputEncoder: Encoder[B] = Encoders.product[B]
  }

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TestAssignmentApp")
  val sc: SparkContext = new SparkContext(conf)
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate
  import spark.implicits._

  val path = "/Users/ibaklashov/Documents/IdeaProjects/git/SparkEx1/src/main/resources/exdata.csv"
  val schema = new StructType()
    .add("category", StringType)
    .add("product", StringType)
    .add("userId", StringType)
    .add("eventTime", TimestampType)
    .add("eventType", StringType)


  val baseDf = spark.read.schema(schema).option("header","true").csv(path).as[RealTableRow]

  baseDf.show(30)



  val dataSeq = Seq(
    TableRow("book", "Scala", 1, 1),
    TableRow("book", "Scala", 1, 2),
    TableRow("book", "Scala", 1, 3),
    TableRow("book", "Scala", 1, 4),
    TableRow("book", "Scala", 1, 10),
    TableRow("book", "Scala", 1, 11),
    TableRow("notebook", "Scala", 2, 2),
    TableRow("PC", "Scala", 1, 14),
    TableRow("PC", "Scala", 1, 15),
    TableRow("PC", "Scala", 1, 30)
  )

  val rawDSS = spark.createDataFrame(dataSeq).as[TableRow]

  val hzcho = MyAgg.toColumn.name("average_salary")
 spark.createDataFrame(baseDf.select(hzcho).collect().head.value)
    .as[SaturatedTableRow]
   // .join(baseDf, Seq("category", "userId", "eventTime"))
  .orderBy("sessionId", "eventTime").show(27)

}
