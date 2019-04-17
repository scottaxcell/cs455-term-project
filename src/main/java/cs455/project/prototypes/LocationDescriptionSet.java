package cs455.project.prototypes;

import cs455.project.crimes.CrimesHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;
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
        if (split.length != CrimesHelper.NUM_FIELDS)
            return "";
        return split[CrimesHelper.LOCATION_DESCRIPTION_INDEX];
    }
}
