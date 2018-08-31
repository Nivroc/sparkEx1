import java.sql.Timestamp
import java.time.LocalDateTime

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.typedLit

import scala.collection.mutable

object Ex1 extends App {

  case class InputRow(userId: Long, date: Timestamp, score: Int)
  case class MarkedRow(userId: Long, date: Timestamp, score: Int, status: String)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TestAssignmentApp")
  val sc: SparkContext = new SparkContext(conf)
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate
  conf.registerKryoClasses(Array(classOf[InputRow], classOf[MarkedRow]))

  val path = s"./src/main/resources/${ConfigFactory.load("application").getString("filename2")}"
  val schema = new StructType()
    .add("userId", LongType)
    .add("date", DateType)
    .add("score", IntegerType)

  import spark.implicits._

  val baseDf: DataFrame = spark.read.schema(schema).option("header", "true").csv(path).repartition($"userId")

  baseDf.cache()
  baseDf.show()

  def markMostRecent(dataframe: DataFrame): DataFrame =
    dataframe
      .withColumn("status", typedLit(""))
      .as[MarkedRow]
      .mapPartitions[MarkedRow] { iter: Iterator[MarkedRow] =>
        {
          val sorted = iter.to[mutable.MutableList].sortWith((x, y) => x.date.before(y.date))
          if (sorted.isEmpty)
            sorted.toIterator
          else
            (sorted.init :+ sorted.last.copy(status = "most_recent")).toIterator
        }
      }
      .toDF()

  import org.apache.spark.sql.functions._

  def markMostRecent2(dataframe: DataFrame): DataFrame =
    dataframe
      .withColumnRenamed("userId", "uid")
      .withColumn("dateAsMs", unix_timestamp($"date"))
      .withColumn("maxDate", max($"dateAsMs") over Window.partitionBy("uid"))
      .withColumn("status", when($"dateAsMs" === $"maxdate", "most_recent").otherwise(""))
      .select("uid", "date", "score", "status")
      .toDF()

  markMostRecent2(baseDf).show(30)
  markMostRecent(baseDf).show(30)
}
