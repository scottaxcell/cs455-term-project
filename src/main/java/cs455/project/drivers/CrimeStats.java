package cs455.project.drivers;

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

/**
 * Collects crime statistics
 * - prints total number of crimes
 * - prints percentage of crime per type
 */
public class CrimeStats extends Driver {

   public static void main(String[] args) {
      new CrimeStats().run();
   }

   private void run() {
      SparkConf conf = new SparkConf().setAppName("Crime Stats");
      JavaSparkContext sc = new JavaSparkContext(conf);

      JavaRDD<String> textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
      List<String> crimes = textFile.filter(CrimesHelper::isValidEntry).map(Utils::splitCommaDelimitedString).map(CrimeStats::getType).filter(Utils::isValidString).collect();

      collateCrimes(crimes);

      saveStatisticsToFile(sc);
   }

   private void saveStatisticsToFile(JavaSparkContext sc) {
      int totalNumCrimes = (int) crimeTypeToCount.values().stream().collect(Collectors.summarizingInt(i -> i)).getSum();

      List<String> writeMe = new ArrayList<>();
      writeMe.add("Crime Statistics");
      writeMe.add("==========================");
      writeMe.add(String.format("Total # crimes: %d", totalNumCrimes));
      writeMe.add("Crime Percentage By Type");
      writeMe.add("------------------------");
      crimeTypeToCount.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(e -> {
         double percentage = Utils.calculatePercentageOfTotal(e.getValue(), totalNumCrimes);
         writeMe.add(String.format("%s (%d): %.2f", e.getKey(), e.getValue(), percentage));
      });

      sc.parallelize(writeMe, 1).saveAsTextFile("CrimeStats");
   }
}
