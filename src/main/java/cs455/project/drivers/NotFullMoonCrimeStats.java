package cs455.project.drivers;

import cs455.project.crimes.CrimesHelper;
import cs455.project.moons.MoonsHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Collects crime statistics on days of a without a full moon
 * - prints total number of crimes
 * - prints percentage of crime per type
 */
public class NotFullMoonCrimeStats extends Driver {

   public static void main(String[] args) {
      new NotFullMoonCrimeStats().run();
   }

   private void run() {
      SparkConf conf = new SparkConf().setAppName("Not Full Moon Crime Stats");
      JavaSparkContext sc = new JavaSparkContext(conf);

      JavaRDD<String> textFile = sc.textFile(Constants.HDFS_MOONS_DIR);
      List<LocalDate> fullMoonDates = textFile.filter(MoonsHelper::isValidEntry).map(Utils::splitCommaDelimitedString).filter(NotFullMoonCrimeStats::isFullMoon).map(NotFullMoonCrimeStats::getDate).filter(Objects::nonNull).collect();

      compileFullMoons(fullMoonDates);

      Broadcast<List<LocalDate>> fullMoonDatesBroadcast = sc.broadcast(this.fullMoonDates);

      textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
      List<String> crimes = textFile.filter(CrimesHelper::isValidEntry).map(Utils::splitCommaDelimitedString).filter(split -> !crimeOccurredOnFullMoon(fullMoonDatesBroadcast, split)).map(NotFullMoonCrimeStats::getType).filter(Utils::isValidString).collect();

      collateCrimes(crimes);

      saveStatisticsToFile(sc);
   }

   private void saveStatisticsToFile(JavaSparkContext sc) {
      int totalNumCrimes = (int) crimeTypeToCount.values().stream().collect(Collectors.summarizingInt(i -> i)).getSum();

      List<String> writeMe = new ArrayList<>();
      writeMe.add("Not Full Moon Crime Statistics");
      writeMe.add("==============================");
      writeMe.add(String.format("Total # crimes: %d", totalNumCrimes));
      writeMe.add("Crime Percentage By Type");
      writeMe.add("------------------------");
      crimeTypeToCount.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(e -> {
         double percentage = Utils.calculatePercentageOfTotal(e.getValue(), totalNumCrimes);
         writeMe.add(String.format("%s (%d): %.2f", e.getKey(), e.getValue(), percentage));
      });

      writeMe.add("\n");
      writeMe.add("Type,Number,% of Total");
      crimeTypeToCount.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(e -> {
         double percentage = Utils.calculatePercentageOfTotal(e.getValue(), totalNumCrimes);
         writeMe.add(String.format("%s,%d,%.2f", e.getKey(), e.getValue(), percentage));
      });

      sc.parallelize(writeMe, 1).saveAsTextFile("NotFullMoonCrimeStats");
   }
}
