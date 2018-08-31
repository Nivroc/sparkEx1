import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StructType}
import org.apache.spark.sql.functions.col

object Ex2 extends App {

  case class InputRow(userId: Long, date: Timestamp, score: Int)

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TestAssignmentApp")
  val sc: SparkContext = new SparkContext(conf)
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate
  conf.registerKryoClasses(Array(classOf[InputRow]))

  val path = s"./src/main/resources/${ConfigFactory.load("application").getString("filename2")}"
  val diffPath = s"./src/main/resources/${ConfigFactory.load("application").getString("diff")}"
  val schema = new StructType()
    .add("userId", LongType)
    .add("date", DateType)
    .add("score", IntegerType)

  import spark.implicits._
  import org.apache.spark.sql.functions._
  val ds1: DataFrame = spark.read.schema(schema).option("header", "true").csv(path)
  val diff: DataFrame = spark.read.schema(schema).option("header", "true").csv(diffPath)

  ds1.cache()
  ds1.show()

  def findDelta(dataset1: DataFrame, dataset2: DataFrame, primaryKey: Seq[String]): DataFrame = {
    dataset1.repartition(primaryKey.map(col): _*)
    dataset2.repartition(primaryKey.map(col): _*)
    dataset1
      .withColumnRenamed("score", "score1")
      .join(dataset2.withColumnRenamed("score", "score2"), primaryKey, "full_outer")
      .withColumn(
        "status",
        when($"score1".isNull, "inserted")
          .otherwise(when($"score1" =!= $"score2", "updated").otherwise(when($"score2".isNull, "deleted")))
      )
      .withColumn("score3", coalesce($"score2", $"score1"))
      .select("userId", "date", "score3", "status").distinct()
      .where($"status".isNotNull)
  }

  findDelta(ds1, diff, Seq("userId", "date")).show(100)

}
