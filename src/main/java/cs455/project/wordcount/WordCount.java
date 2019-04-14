package cs455.project.wordcount;

import cs455.project.Constants;
import cs455.project.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args){

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Word Count");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD<String> textFile = sc.textFile(String.format("hdfs://%s/cs455/project/dummy/MobyDick.txt", Constants.HDFS_SERVER));
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        Utils.debug("Total words: " + counts.count());
        counts.saveAsTextFile("wordcount-out"); //defaults to /user/${USER}/wordcount-cout
    }
}
