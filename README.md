# CS 455 Term Project
CS 455 Term Project

## Hadoop/YARN
.bashrc

`
export HADOOP_CONF_DIR=${HOME}/cs455TermProject/hadoop-conf
`

Start and stop
```
./scripts/start-dfs.sh && ./scripts/start-yarn.sh
./scripts/stop-yarn.sh && ./scripts/stop-dfs.sh
```

Hadoop Web UI - http://phoenix:54301/dfshealth.html#tab-overview

YARN Web UI - http://phoenix:54307/cluster

Datasets

```
$ $HADOOP_HOME/bin/hdfs dfs -ls /cs455/project
Found 2 items
drwxr-xr-x   - sgaxcell supergroup          0 2019-04-13 11:35 /cs455/project/crimes
drwxr-xr-x   - sgaxcell supergroup          0 2019-04-13 11:35 /cs455/project/moons
```

## Spark
.bashrc

`
export SPARK_HOME=${HOME}/cs455TermProject/spark-2.4.1-bin-hadoop2.7
`

Start and stop
```
$SPARK_HOME/sbin/start-all.sh
$SPARK_HOME/sbin/stop-all.sh
```
Web UI - http://phoenix:30312

Word Count example
```
$SPARK_HOME/bin/spark-submit --master spark://phoenix:30311 --deploy-mode cluster --class WordCount --supervise build/libs/cs455TermProject.jar
```
