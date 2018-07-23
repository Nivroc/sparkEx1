import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.Logger
import org.apache.log4j.Level

object SqlSolution {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TestAssignmentApp")
  val sc: SparkContext = new SparkContext(conf)
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate

  def main(args: Array[String]) {

    val path = s"./src/main/resources/${ConfigFactory.load("application").getString("filename")}"
    val baseDf = spark.read.option("header", "true").csv(path)
    baseDf.createOrReplaceTempView("exdata")

    //Main query, saturating data with sessions. Session break condition may vary according to the task.

    def mainQuery(sessionCond: String) = s"""WITH
      |p1 AS ( SELECT category, product, userId,
      |cast(to_utc_timestamp(eventTime, 'PST') AS int) AS op_date,
      |eventType,
      |LAG(eventTime) OVER (PARTITION BY category, userid ORDER BY category, userId, eventtime) AS lagg
      |FROM exdata),
      |
      |p2 AS ( SELECT *,
      |(op_date - cast(to_utc_timestamp(lagg, 'PST') AS int)) AS lag_seconds
      |FROM p1),
      |
      |p3 AS (SELECT category, product, userId, op_date, eventType, cast(lagg AS int), lag_seconds,
      |CASE WHEN $sessionCond THEN 1 ELSE 0 END AS session_break
      |FROM p2),
      |
      |p33 as (
      |SELECT *, SUM(session_break) OVER (ORDER BY category, userId, op_date range between unbounded preceding and current row) as rng FROM p3
      |),
      |
      |p4 AS (
      |SELECT *,
      |FIRST_VALUE(op_date) OVER (PARTITION BY category, userid, rng ORDER BY op_date) AS session_start ,
      |LAST_VALUE(op_date) OVER (PARTITION BY category, userid, rng ORDER BY op_date, userid RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_end
      |FROM p33
      |)
      |
      |SELECT category, product, userId, cast(op_date AS timestamp), eventType,
      |cast(session_start AS timestamp) ,
      |cast(session_end AS timestamp) ,
      |dense_rank() OVER (ORDER BY session_end) AS session_id
      |FROM p4
      |ORDER BY session_id
      |""".stripMargin

    //Session breaks if the delay is longer, than 5 mins
    val firstWaySession = spark.sql(mainQuery("lag_seconds >= 300"))
    firstWaySession.cache()
    firstWaySession.show(30)
    firstWaySession.createOrReplaceTempView("saturated_data")

    //Session breaks if the delay is longer, than 5 mins, or the product in focus changes
    val secondWaySession =
      spark.sql(mainQuery("(lag_seconds >= 300 or product != lag(product) over(ORDER BY op_date))"))
    secondWaySession.cache()
    secondWaySession.show(30)
    secondWaySession.createOrReplaceTempView("saturated_data2")

    //Average time
    spark.sql(s"""SELECT distinct
         |category,
         |round(avg(cast(cast(session_end AS int) - cast(session_start AS int) AS timestamp)), 2) AS average_time
         |FROM saturated_data
         |GROUP BY category
       """.stripMargin).show(30)

    //Median time. If number of sessions is even then median is calculated as average of two middle-numbered values.

    spark.sql(s"""with a1 as ( SELECT distinct
         |category,
         |(cast(session_end AS int) - cast(session_start AS int)) AS duration
         |FROM saturated_data
         |order by category),
         |a2 as (
         |select category,
         |duration,
         |dense_rank() over (partition by category order by duration) as rnk from a1),
         |a3 as (select category, duration, rnk, max(rnk) over (partition by category) as mxrnk from a2),
         |a4 as (select
         |category, duration, rnk, mxrnk, lead(duration) over (partition by category order by duration) as ld,
         |case when mxrnk % 2 == 0 then
         |cast ((mxrnk/2) as int)
         |else
         |cast((mxrnk/2 + 0.5) as int)
         |end as medval
         |from a3)
         |select category,
         |case when mxrnk % 2 == 0 then
         |round((duration + ld) / 2, 1)
         |else duration
         |end as median
         |from a4 where rnk = medval
       """.stripMargin).show(30)

    //Less, than a minute, 1-5, more, than 5 mins
    spark.sql(s"""WITH a1 AS
         |(SELECT distinct
         |category,
         |session_id,
         |userid,
         |round(avg(cast(cast(session_end AS int) - cast(session_start AS int) AS timestamp)),2) as duration
         |FROM saturated_data
         |GROUP BY category, session_id, userid),
         |a2 AS (
         |SELECT category, userid, duration,
         |CASE
         |WHEN duration < 60 THEN '< 1 min'
         |WHEN duration >= 60 and duration <= 300 THEN '1 - 5 min'
         |else '> 5 mins'
         |end cat
         |FROM a1)
         |SELECT distinct category, cat, count(userid)
         |FROM a2
         |GROUP BY category, cat
         |ORDER BY category, cat
       """.stripMargin).show(30)

    //Product ranking by category by time spent
    spark
      .sql("""
        |WITH timed AS (
        |SELECT category, product, userid, session_id,
        |round(avg(cast(cast(session_end AS int) - cast(session_start AS int) AS timestamp)),2) as duration
        |FROM saturated_data2
        |GROUP BY category, userid, session_id, product
        |),
        |sorted AS (
        |SELECT category, product, sum(duration) as time
        |FROM timed
        |GROUP BY category, product
        |ORDER BY category, time desc),
        |final AS (
        |SELECT *, row_number() over (PARTITION BY category ORDER BY time DESC) AS rn FROM sorted
        |)
        |SELECT category, product FROM final where rn <= 10
      """.stripMargin)
      .show()

    sc.stop()

  }
}
