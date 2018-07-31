import java.sql.Timestamp
import java.time.temporal.ChronoUnit

import AggregatorSolution.{RealTableRow, SaturatedTableRow}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

object Deprecated {

  /*
  val aggregate = MyAgg.toColumn.name("aggregated")
  spark
    .createDataFrame(baseDf.select(aggregate).collect().head)
    .as[SaturatedTableRow]
    .join(baseDf, Seq("category", "userId", "eventTime"), "left_outer")
    .distinct()
    .orderBy("sessionId", "eventTime")
    .show(30)
   */

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
  }}
