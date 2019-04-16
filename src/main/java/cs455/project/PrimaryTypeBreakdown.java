package cs455.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrimaryTypeBreakdown {
    private static Map<String, Integer> primaryTypeToCount = new HashMap<>();

    public static void main(String[] args) {
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
        if (split.length != CrimesIndices.NUM_FIELDS)
            return "";
        return split[CrimesIndices.PRIMARY_TYPE_INDEX];
    }

    private static void breakdownCrimes(List<String> crimes) {
        crimes.stream()
                .forEach(PrimaryTypeBreakdown::incrementCount);
    }

    private static void incrementCount(String key) {
        int count = primaryTypeToCount.containsKey(key) ? primaryTypeToCount.get(key) : 0;
        primaryTypeToCount.put(key, count + 1);
    }

    private static void saveCrimeBreakdownToFile(JavaSparkContext sc, Map<String, Integer> primaryTypeToCount) {
        int totalNumCrimes = (int) primaryTypeToCount.values().stream()
                .collect(Collectors.summarizingInt(i -> i)).getSum();

        List<String> writeMe = new ArrayList<>();
        primaryTypeToCount.entrySet().stream()
                .forEach(e -> {
                    double percentage = calculatePercentageOfTotal(totalNumCrimes, e.getValue());
                    writeMe.add(String.format("%s : %.4f", e.getKey(), percentage));
                });

        sc.parallelize(writeMe, 1)
                .saveAsTextFile("primaryTypeBreakdown");
    }

    private static double calculatePercentageOfTotal(int total, int value) {
        return total / value;
    }
}
