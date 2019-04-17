#!/bin/env bash
#

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/primaryTypeSet
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.prototypes.PrimaryTypeSet \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/locationDescriptionSet
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.prototypes.LocationDescriptionSet \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/primaryTypeBreakdown
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.prototypes.PrimaryTypeBreakdown \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/daysOfFullMoon
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.prototypes.DaysOfFullMoon \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/FullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.drivers.FullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar