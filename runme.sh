#!/bin/env bash

PERSONAL_SPARK=phoenix:30318 # Scott's spark
#PERSONAL_SPARK=topeka:50318 # Matt's spark
#PERSONAL_SPARK=olympia:30318 # Daniel's spark

gradle assemble

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/CrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.CrimeStats \
--supervise build/libs/cs455TermProject.jar

HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekConfidenceIntervalFullMoon
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekConfidenceIntervalFullMoon \
--supervise build/libs/cs455TermProject.jar

HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekConfidenceIntervalFullMoonInside
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekConfidenceIntervalFullMoonInside \
--supervise build/libs/cs455TermProject.jar

HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekConfidenceIntervalFullMoonOutside
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekConfidenceIntervalFullMoonOutside \
--supervise build/libs/cs455TermProject.jar

HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekConfidenceIntervalNotFullMoon
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekConfidenceIntervalNotFullMoon \
--supervise build/libs/cs455TermProject.jar

HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekConfidenceIntervalNotFullMoonInside
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekConfidenceIntervalNotFullMoonInside \
--supervise build/libs/cs455TermProject.jar

HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekConfidenceIntervalNotFullMoonOutside
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekConfidenceIntervalNotFullMoonOutside \
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

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekCrimeStats \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekCrimeStatsWithFullMoon
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekCrimeStatsWithFullMoon \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/DayOfWeekNonFullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.DayOfWeekNonFullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar