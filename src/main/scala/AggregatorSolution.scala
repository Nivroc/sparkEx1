import java.sql.Timestamp
import java.time.temporal.ChronoUnit

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

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

  case class Session(
    category: String,
    userId: String,
    sessionStart: Timestamp,
    sessionEnd: Timestamp,
    times: Seq[Timestamp]
  )

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

  import org.apache.spark.sql.functions._
  case class KVTimestamp(category: String, userId: String, time: Timestamp)

  val result = baseDf
    .repartition($"category", $"userId")
    .mapPartitions(iter => {
      iter
        .foldLeft(Seq.empty[Seq[KVTimestamp]])(
          (s, e) =>
            if (s.nonEmpty && s.last.last.time.toLocalDateTime
                .plus(5, ChronoUnit.MINUTES)
                .isAfter(e.eventTime.toLocalDateTime))
              s.init :+ (s.last :+ KVTimestamp(e.category, e.userId, e.eventTime))
            else
              s :+ Seq(KVTimestamp(e.category, e.userId, e.eventTime))
        )
        .toIterator
        .map(sq => Session(sq.head.category, sq.head.userId, sq.head.time, sq.last.time, sq.map(_.time)))
    })
    .withColumn("id", monotonically_increasing_id())
    .withColumn("eventTime", explode($"times"))
    .drop($"times")
    .join(baseDf, Seq("category", "userId", "eventTime"), "left_outer")

    result.cache()
    result.explain()
    result.show()

}
