import org.apache.spark.{SparkConf, SparkContext}

object ex1 {

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TestAssignmentApp")
  val sc: SparkContext = new SparkContext(conf)
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate

  def main(args: Array[String]) {

    val path = "/Users/ibaklashov/Documents/IdeaProjects/git/SparkEx1/src/main/resources/exdata.csv"
    val baseDf = spark.read.option("header","true").csv(path)
    baseDf.createOrReplaceTempView("exdata")

    //По сути запрос на первое упражнение. Функция т.к. условие для обрыва сессии переменное как это требуется для
    // последнего задания
    def mainQuery(sessionCond: String) = s"""WITH
      |p1 AS ( SELECT category, product, userId,
      |cast(to_utc_timestamp(eventTime, 'PST') AS int) AS op_date,
      |eventType,
      |lag(eventTime) OVER (PARTITION BY category, userid ORDER BY eventtime) AS lagg
      |FROM exdata),
      |
      |p2 AS ( SELECT *,
      |(op_date - cast(to_utc_timestamp(lagg, 'PST') AS int)) AS lag_seconds
      |FROM p1),
      |
      |p3 AS (SELECT category, product, userId, op_date, eventType, cast(lagg AS int), lag_seconds,
      |CASE WHEN $sessionCond THEN 1 else 0 end AS session_break
      |FROM p2),
      |
      |p33 as (
      |SELECT *, sum(session_break) over(ORDER BY op_date range between unbounded preceding and current row) as rng FROM p3
      |),
      |
      |p4 AS (
      |SELECT *,
      |first_value(op_date) OVER (PARTITION BY category, userid, rng ORDER BY op_date) AS session_start,
      |last_value(op_date) OVER (PARTITION BY category, userid, rng ORDER BY op_date, userid RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_end
      |FROM p33
      |)
      |
      |SELECT category, product, userId, cast(op_date AS timestamp), eventType,
      |cast(session_start AS timestamp),
      |cast(session_end AS timestamp),
      |dense_rank() OVER (ORDER BY session_end) AS session_id
      |FROM p4
      |ORDER BY session_id
      |""".stripMargin

      //обрыв сессий только если больше 5 минут
    val firstWaySession = spark.sql(mainQuery("lag_seconds >= 300"))
    firstWaySession.cache()
    firstWaySession.show(30)
    firstWaySession.createOrReplaceTempView("saturated_data")

    //обрыв сессий если больше 5 минут или сменился продукт
    val secondWaySession = spark.sql(mainQuery("(lag_seconds >= 300 or product != lag(product) over(ORDER BY op_date))"))
    secondWaySession.cache()
    secondWaySession.show(30)
    secondWaySession.createOrReplaceTempView("saturated_data2")

    //среднее время
    spark.sql(
      s"""SELECT distinct
         |category,
         |round(avg(cast(cast(session_end AS int) - cast(session_start AS int) AS timestamp)), 2) AS median_duration
         |FROM saturated_data
         |GROUP BY category
       """.stripMargin).show(30)

    //кто меньше минуты, кто 1-5, кто больше 5ти минут
    spark.sql(
      s"""WITH a1 AS
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

    //ранг по времени проведённому за продуктом в рамкаx одной сессии
    spark.sql(
      """
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
      """.stripMargin
    ).show()

    sc.stop()

  }
}
