#!/bin/env bash
#

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/primaryTypeSet
gradle build && \
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.PrimaryTypeSet \
--supervise build/libs/cs455TermProject.jar

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/locationDescriptionSet
gradle build && \
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.LocationDescriptionSet \
--supervise build/libs/cs455TermProject.jar