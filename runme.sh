#!/bin/env bash
#

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/primaryTypeSet
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.drivers.PrimaryTypeSet \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/locationDescriptionSet
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.drivers.LocationDescriptionSet \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/primaryTypeBreakdown
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.drivers.PrimaryTypeBreakdown \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/daysOfFullMoon
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.drivers.DaysOfFullMoon \
--supervise build/libs/cs455TermProject.jar