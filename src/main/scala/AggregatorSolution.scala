import java.sql.Timestamp
import java.time.temporal.ChronoUnit

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object AggregatorSolution extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  case class RealTableRow(category: String, product: String, userId: String, eventTime: Timestamp, eventType: String)

  case class SaturatedTableRow(
    category: String,
    userId: String,
    eventTime: Timestamp,
    sessionId: Int,
    sessionStart: Timestamp,
    sessionEnd: Timestamp
  )

  object MyAgg extends Aggregator[RealTableRow, Map[(String, String), Seq[Timestamp]], Seq[SaturatedTableRow]] {

    override def zero: Map[(String, String), Seq[Timestamp]] = Map.empty[(String, String), Seq[Timestamp]]

    override def reduce(
      b: Map[(String, String), Seq[Timestamp]],
      a: RealTableRow
    ): Map[(String, String), Seq[Timestamp]] =
      b.updated((a.category, a.userId), b.getOrElse((a.category, a.userId), Nil) :+ a.eventTime)

    override def merge(
      b1: Map[(String, String), Seq[Timestamp]],
      b2: Map[(String, String), Seq[Timestamp]]
    ): Map[(String, String), Seq[Timestamp]] =
      (b1.keySet ++ b2.keySet).map(key => (key, b1.getOrElse(key, Nil) ++ b2.getOrElse(key, Nil))).toMap

    //formatter:off
    override def finish(reduction: Map[(String, String), Seq[Timestamp]]): Seq[SaturatedTableRow] = {
      val st1 = reduction.map(
        kv =>
          kv._1 -> kv._2
            .sortWith((x, y) => x.before(y))
            .foldLeft(Seq.empty[Seq[Timestamp]])(
              (s, e) =>
                if (s.nonEmpty && s.last.last.toLocalDateTime.plus(5, ChronoUnit.MINUTES).isAfter(e.toLocalDateTime))
                  s.init :+ (s.last :+ e)
                else
                  s :+ Seq(e)
          )
      )
      val globalIds = st1.values.toSeq.flatten.zipWithIndex.toMap
      st1.foldLeft(Seq.empty[SaturatedTableRow])(
        (sq, kv) =>
          sq ++ kv._2
            .flatMap(sq => sq.map(time => SaturatedTableRow(kv._1._1, kv._1._2, time, globalIds(sq), sq.head, sq.last)))
      )
      //formatter:on
    }

    override def bufferEncoder: Encoder[Map[(String, String), Seq[Timestamp]]] =
      Encoders.kryo[Map[(String, String), Seq[Timestamp]]]

    override def outputEncoder: Encoder[Seq[SaturatedTableRow]] = Encoders.kryo[Seq[SaturatedTableRow]]
  }

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TestAssignmentApp")
  val sc: SparkContext = new SparkContext(conf)
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate
  conf.registerKryoClasses(Array(classOf[RealTableRow], classOf[SaturatedTableRow]))

  import spark.implicits._

  val path = s"./src/main/resources/${ConfigFactory.load("application").getString("filename")}"
  val schema = new StructType()
    .add("category", StringType)
    .add("product", StringType)
    .add("userId", StringType)
    .add("eventTime", TimestampType)
    .add("eventType", StringType)

  val baseDf = spark.read.schema(schema).option("header", "true").csv(path).as[RealTableRow]

  val aggregate = MyAgg.toColumn.name("aggregated")
  spark
    .createDataFrame(baseDf.select(aggregate).collect().head)
    .as[SaturatedTableRow]
    .join(baseDf, Seq("category", "userId", "eventTime"), "left_outer")
    .distinct()
    .orderBy("sessionId", "eventTime")
    .show(30)

}
