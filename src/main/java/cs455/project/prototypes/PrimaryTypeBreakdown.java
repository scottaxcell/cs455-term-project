package cs455.project.prototypes;

import cs455.project.crimes.CrimesHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrimaryTypeBreakdown {
    private Map<String, Integer> primaryTypeToCount = new HashMap<>();

    public static void main(String[] args) {
        new PrimaryTypeBreakdown().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Primary Type Breakdown");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
        List<String> allCrimes = textFile
                .map(PrimaryTypeBreakdown::getPrimaryType)
                .filter(Utils::isValidString)
                .collect();

        breakdownCrimes(allCrimes);

        saveCrimeBreakdownToFile(sc, primaryTypeToCount);
    }

    private static String getPrimaryType(String s) {
        String[] split = Utils.splitCommaDelimitedString(s);
        if (split.length != CrimesHelper.NUM_FIELDS)
            return "";
        return split[CrimesHelper.PRIMARY_TYPE_INDEX];
    }

    private void breakdownCrimes(List<String> crimes) {
        crimes.stream()
                .forEach(this::incrementCount);
    }

    private void incrementCount(String key) {
        int count = primaryTypeToCount.containsKey(key) ? primaryTypeToCount.get(key) : 0;
        primaryTypeToCount.put(key, count + 1);
    }

    private static void saveCrimeBreakdownToFile(JavaSparkContext sc, Map<String, Integer> primaryTypeToCount) {
        int totalNumCrimes = (int) primaryTypeToCount.values().stream()
                .collect(Collectors.summarizingInt(i -> i)).getSum();

        List<String> writeMe = new ArrayList<>();
        primaryTypeToCount.entrySet().stream()
                .forEach(e -> {
                    double percentage = Utils.calculatePercentageOfTotal(e.getValue(), totalNumCrimes);
                    writeMe.add(String.format("%s : %.4f", e.getKey(), percentage));
                });

        sc.parallelize(writeMe, 1)
                .saveAsTextFile("primaryTypeBreakdown");
    }


}
