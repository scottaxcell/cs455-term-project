#!/bin/env bash

PERSONAL_SPARK=phoenix:30318

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/FullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://$PERSONAL_SPARK \
--deploy-mode cluster \
--class cs455.project.drivers.FullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar