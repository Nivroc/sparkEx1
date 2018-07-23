This is a solution for sample task, which involved:
1. Saturating data with sessions, based on time lags and product changes. This task was done, using two different methods:
    - plain SQL, based on DF
    - Spark Aggregator
2. In the second part of the task the median session time, user count by session duration and top products metrics were
    calculated, using plain SQL

How to run:
    1. put a file with dataset in src/main/resources
    2. change 'filename' parameter in /resources/application.conf config file to your file name.

Run SqlSolution.scala as simple scala App
Run AggregatorSolution.scala as simple scala App

All results will be shown in stdout (first 30 rows of each).
