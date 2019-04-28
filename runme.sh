#!/bin/env bash

PERSONAL_SPARK=phoenix:30318

gradle assemble

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/CrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.CrimeStats \
--supervise build/libs/cs455TermProject.jar

HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekCrimeStats \
--supervise build/libs/cs455TermProject.jar

HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekCrimeStatsWithFullMoon
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekCrimeStatsWithFullMoon \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/FullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.FullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/IndoorFullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.IndoorFullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/IndoorNotFullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.IndoorNotFullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/NotFullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.NotFullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/OutdoorFullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.OutdoorFullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/OutdoorNotFullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.OutdoorNotFullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar
