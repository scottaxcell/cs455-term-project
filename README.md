# CS 455 Term Project
CS 455 Term Project

## Setup and Running Spark Drivers
### Setup
1. Clone the git repo to your home directory. If not, you'll need to change the paths below.
2. Add the following lines to your .bashrc
```
export HADOOP_CONF_DIR=${HOME}/cs455TermProject/hadoop-conf
export SPARK_HOME=${HOME}/cs455TermProject/spark-2.4.1-bin-hadoop2.7
```
### Running a Driver
To run the FullMoonCrimeStats driver execute the following commands.
```
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/FullMoonCrimeStats
$SPARK_HOME/bin/spark-submit \
--master spark://phoenix:30318 \
--deploy-mode cluster \
--class cs455.project.drivers.FullMoonCrimeStats \
--supervise build/libs/cs455TermProject.jar
```
The output for the driver is written to the /user/${USER}/FullMoonCrimeStats in HDFS.

You can view the results via the Hadoop web UI - http://phoenix:54301/dfshealth.html#tab-overview 

You can monitor your Spark jobs via the Spark web UI - http://phoenix:30319

### Running a Driver Locally
In addition, you can run a Spark driver locally for the purposes of debugging.
To do this uncomment the commented lines in cs455.project.drivers.FullMoonCrimeStats.run and comment the lines above them.
See lines 32, 36, 50. I've opened permission on those datafiles so you should be able to read them.
Let me know if you can't.

Execute cs455.project.drivers.FullMoonCrimeStats.main via your favorite IDE.

The results when running locally will be written to your cwd rather than HDFS.

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
Web UI - http://phoenix:30319

Word Count example
```
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/${USER}/wordcount-out
$SPARK_HOME/bin/spark-submit --master spark://phoenix:30318 --deploy-mode cluster --class cs455.project.wordcount.WordCount --supervise build/libs/cs455TermProject.jar
```

## Dataset Formats    
Crimes:
```
ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location
2852538,HJ513504,07/22/2003 06:00:00 PM,003XX W 47TH ST,051A,ASSAULT,AGGRAVATED: HANDGUN,BARBERSHOP,false,false,0935,009,3,37,04A,1174842,1873793,2003,02/10/2018 03:50:01 PM,41.809076327,-87.634232572,"(41.809076327, -87.634232572)"
```
Moons:
```
date,time,phase,phaseid,datetime,timestamp,friendlydate
01/05/2019,08:28 PM,New Moon,1,2019-01-05 20:28:00,1546738080,"January 5, 2019"
```
                                            
