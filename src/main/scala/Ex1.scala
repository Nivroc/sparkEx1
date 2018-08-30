import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.typedLit

import scala.collection.mutable

object Ex1 extends App {

  case class InputRow(userId: Long, date: Timestamp, score: Int)
  case class MarkedRow(userId: Long, date: Timestamp, score: Int, status: String)

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
      .sortWithinPartitions("date")
      .mapPartitions[MarkedRow] { iter: Iterator[MarkedRow] =>
        {
          val sorted = iter.to[mutable.MutableList].sortWith((x, y) => x.date.before(y.date))
          if(sorted.isEmpty)
            sorted.toIterator
          else
            (sorted.init :+ sorted.last.copy(status = "most_recent")).toIterator
        }
      }
      .toDF()

  markMostRecent(baseDf).show(30)
}
