package cs455.project.prototypes;

import cs455.project.crimes.CrimesHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;


public class PrimaryTypeSet {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Primary Type Set");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
        List<String> uniquePrimaryTypes = textFile
                .map(PrimaryTypeSet::getPrimaryType)
                .filter(Utils::isValidString)
                .distinct()
                .sortBy(s -> s, true, 1)
                .collect();

        sc.parallelize(uniquePrimaryTypes, 1)
                .saveAsTextFile("primaryTypeSet");
    }

    private static String getPrimaryType(String s) {
        String[] split = Utils.splitCommaDelimitedString(s);
        if (split.length != CrimesHelper.NUM_FIELDS)
            return "";
        return split[CrimesHelper.PRIMARY_TYPE_INDEX];
    }
}
