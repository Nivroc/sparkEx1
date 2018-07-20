import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Aggregator

object tstAggregator extends App {

  case class MyRow(key: Key, product: String, time: Int)
  case class TableRow(category: String, product: String, userId: Int, time: Int)
  case class SaturatedTableRow(category: String, userId: Int, time:Int, sessionId:Int, sessionStart: Int, sessionEnd:Int)
  case class Session(id: Int, start: Int, finish: Int)
  case class Key(category: String, userId: Int)
  case class TimedKey(category: String, userId: Int, time: Int)
  case class IdKey(sid: Int, category: String, userId: Int)
  case class NewRow(category: String, product: String, userId: Int, time: Int, start: Int, finish: Int)
  case class A(value :Map[Key, Seq[Int]])
  case class B(value :Seq[SaturatedTableRow])

  object MyAgg extends Aggregator[MyRow, A, B] {

    override def zero: A = A(Map.empty[Key, Seq[Int]])

    override def reduce(b: A, a: MyRow): A = A(b.value.updated(a.key, b.value.getOrElse(a.key, Nil) :+ a.time))

    override def merge(b1: A, b2: A): A =
      A((b1.value.keySet ++ b2.value.keySet).map(key => (key, b1.value.getOrElse(key, Nil) ++ b2.value.getOrElse(key, Nil))).toMap)

    override def finish(reduction: A): B = {
      val st1 = reduction.value.map(kv => kv._1 -> kv._2.sorted.foldLeft(Seq.empty[Seq[Int]])((s, e) =>
        if (s.nonEmpty && s.last.last + 3 >= e) s.init :+ (s.last :+ e) else s :+ Seq(e)))
      val globalids = st1.values.toSeq.flatten.zipWithIndex.toMap
      val ided = st1.foldLeft(Map.empty[IdKey, Seq[Int]])((mp, kv) =>
        mp ++ kv._2.map(sq => (IdKey(globalids(sq), kv._1.category, kv._1.userId), sq)).toMap)
      val stEnd = ided.map(kv => kv._2 -> (Key(kv._1.category, kv._1.userId), Session(kv._1.sid, kv._2.head, kv._2.last)))
      B(stEnd.foldLeft(Seq.empty[SaturatedTableRow])((mp, e) =>
        mp ++ e._1.map(time => SaturatedTableRow(e._2._1.category, e._2._1.userId, time, e._2._2.id, e._2._2.start, e._2._2.finish))))

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

  val rawDS = spark.createDataFrame(dataSeq.map(x => MyRow(Key(x.category, x.userId), x.product, x.time))).as[MyRow]

  val rawDSS = spark.createDataFrame(dataSeq).as[TableRow]

  val hzcho = MyAgg.toColumn.name("average_salary")
  spark.createDataFrame(rawDS.select(hzcho).collect().head.value)
    .as[SaturatedTableRow]
    .join(rawDSS, Seq("category", "userId", "time")).show()

}
