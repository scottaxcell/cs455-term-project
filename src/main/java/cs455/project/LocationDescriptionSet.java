package cs455.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;


public class LocationDescriptionSet {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Location Description Set");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
        List<String> uniqueLocationDescriptions = textFile
                .map(LocationDescriptionSet::getLocationDescription)
                .filter(Utils::isValidString)
                .distinct()
                .sortBy(s -> s, true, 1)
                .collect();

        sc.parallelize(uniqueLocationDescriptions, 1)
                .saveAsTextFile("locationDescriptionSet");
    }

    private static String getLocationDescription(String s) {
        String[] split = Utils.splitCommaDelimitedString(s);
        if (split.length != CrimesIndices.NUM_FIELDS)
            return "";
        return split[CrimesIndices.LOCATION_DESCRIPTION_INDEX];
    }
}
