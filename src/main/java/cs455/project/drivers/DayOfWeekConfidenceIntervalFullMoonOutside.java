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
import java.util.stream.Stream;

public class DayOfWeekConfidenceIntervalFullMoonOutside extends DayOfWeekConfidenceIntervalNotFullMoon {

   public static void main(String[] args) {
      new DayOfWeekConfidenceIntervalFullMoonOutside().run();
   }

   void run() {
      //SparkConf conf = new SparkConf().setAppName("Day Of Week Confidence Interval Application With Full Moon Outside");
      SparkConf conf = new SparkConf().setMaster("local").setAppName("Day Of Week Confidence Interval Application With Full Moon Outside");

      JavaSparkContext sc = new JavaSparkContext(conf);

      //JavaRDD<String> textFile = sc.textFile(Constants.HDFS_MOONS_DIR);
      JavaRDD<String> textFile = sc.textFile("/Users/mmuller/Downloads/moons/moon-phases-*.csv");

      List<LocalDate> fullMoonDates = textFile.filter(MoonsHelper::isValidEntry).map(Utils::splitCommaDelimitedString).filter(DayOfWeekConfidenceIntervalNotFullMoon::isFullMoon).map(DayOfWeekConfidenceIntervalNotFullMoon::getDate).filter(Objects::nonNull).collect();

      compileFullMoons(fullMoonDates);

      Broadcast<List<LocalDate>> fullMoonDatesBroadcast = sc.broadcast(this.fullMoonDates);
      List<String> outdoorLocations = Stream.of(CrimesHelper.OUTDOOR_LOCATIONS).collect(Collectors.toList());
      Broadcast<List<String>> outdoorLocationsBroadcast = sc.broadcast(outdoorLocations);

      //textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
      textFile = sc.textFile("/Users/mmuller/Downloads/chicagoCrimes2001ToPresent.csv");

      JavaRDD<String[]> fil = textFile.filter(CrimesHelper::isValidEntry)
            .map(Utils::splitCommaDelimitedString)
            .filter(split -> crimeOccurredOnFullMoon(fullMoonDatesBroadcast, split))
            .filter(split -> OutdoorFullMoonCrimeStats.crimeOccurredOutdoors(outdoorLocationsBroadcast, split));
      Map<String, Integer> flatCount = fil.mapToPair(this::dateTransform).reduceByKey((a, b) -> a + b).collectAsMap();
      breakUpFlatCount(flatCount);

      this.crimes.entrySet().forEach(this::buildBaseStats); // count totals
      this.stats.values().forEach(CrimesToday::calcAverages); // calculate averages
      this.crimes.entrySet().forEach(this::buildVariance); // calculate variance
      this.stats.values().forEach(CrimesToday::calcConfIntv); // calculate confidence interval

      saveStatisticsToFile(sc);
   }

   private void saveStatisticsToFile(JavaSparkContext sc) {

      List<String> writeMe = new ArrayList<>();
      writeMe.add("Day of Week Crime .95 Confidence Interval Full Moon Outside");
      writeMe.add("===========================================");
      writeMe.add("These are full moon days.  If Their Crime Level Averages are outside of Confidence Interval it's Abnormal");
      writeMe.add("------------------------------------------------------------------------");

      for (String s : Constants.DAYS_OF_WEEK) {
         if (Utils.isValidString(s)) {
            CrimesToday crimeStats = this.stats.get(s);
            writeMe.add("============");
            writeMe.add(String.format("%s (Occurrences %d)", crimeStats.weekday, crimeStats.numberOfThisWeekDay));
            writeMe.add("============");

            crimeStats.dailyCrimeConfidenceInterval.entrySet().forEach(crime -> {
               String key = crime.getKey();
               writeMe.add(String.format("%s Occurrences %d, Average %.2f ± %.2f, .95 Confidence Interval %.2f ± %.2f", key, crimeStats.dailyCrimeCount.get(key), crimeStats.dailyCrimeAve.get(key), crimeStats.dailyCrimeStd.get(key), crimeStats.dailyCrimeAve.get(key), crime.getValue()));
            });
            writeMe.add("\n");

            writeMe.add("Day Of Week,Type,Number,Ave,StdDev,ConfIntv");
            crimeStats.dailyCrimeConfidenceInterval.entrySet().forEach(crime -> {
               String key = crime.getKey();
               writeMe.add(String.format("%s,%s,%d,%.2f,%.2f,%.2f", crimeStats.weekday, key, crimeStats.dailyCrimeCount.get(key), crimeStats.dailyCrimeAve.get(key), crimeStats.dailyCrimeStd.get(key), crime.getValue()));
            });
            writeMe.add("\n");
         }
      }

      sc.parallelize(writeMe, 1).saveAsTextFile("DayOfWeekConfidenceIntervalFullMoonOutside");
   }
}
