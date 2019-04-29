package cs455.project.drivers;

import cs455.project.crimes.CrimesHelper;
import cs455.project.moons.MoonsHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DayOfWeekConfidenceIntervalNotFullMoonOutside extends Driver implements Serializable {

   // 01/05/2008, Arson, 1
   Map<LocalDate, Map<String, Integer>> crimes = new HashMap<>();

   // Sunday, <Number of Sundays>, Arson, <Arson on Sunday Count>, <Average Arson on Sunday>, <Std Dev>, <Conf Intv>
   Map<String, CrimesToday> stats = new HashMap<>();

   public static void main(String[] args) {
      new DayOfWeekConfidenceIntervalNotFullMoonOutside().run();
   }

   void run() {
      //SparkConf conf = new SparkConf().setAppName("Day Of Week Confidence Interval Application Without Full Moon Outside");
      SparkConf conf = new SparkConf().setMaster("local").setAppName("Day Of Week Confidence Interval Application Without Full Moon Outside");

      JavaSparkContext sc = new JavaSparkContext(conf);

      //JavaRDD<String> textFile = sc.textFile(Constants.HDFS_MOONS_DIR);
      JavaRDD<String> textFile = sc.textFile("/Users/mmuller/Downloads/moons/moon-phases-*.csv");

      List<LocalDate> fullMoonDates = textFile.filter(MoonsHelper::isValidEntry).map(Utils::splitCommaDelimitedString).filter(DayOfWeekConfidenceIntervalNotFullMoonOutside::isFullMoon).map(DayOfWeekConfidenceIntervalNotFullMoonOutside::getDate).filter(Objects::nonNull).collect();

      compileFullMoons(fullMoonDates);

      Broadcast<List<LocalDate>> fullMoonDatesBroadcast = sc.broadcast(this.fullMoonDates);
      List<String> outdoorLocations = Stream.of(CrimesHelper.OUTDOOR_LOCATIONS).collect(Collectors.toList());
      Broadcast<List<String>> outdoorLocationsBroadcast = sc.broadcast(outdoorLocations);

      //textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
      textFile = sc.textFile("/Users/mmuller/Downloads/chicagoCrimes2001ToPresent.csv");

      JavaRDD<String[]> fil = textFile.filter(CrimesHelper::isValidEntry)
            .map(Utils::splitCommaDelimitedString)
            .filter(split -> !crimeOccurredOnFullMoon(fullMoonDatesBroadcast, split))
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
      writeMe.add("Day of Week Crime .95 Confidence Interval Not Full Moon Outside");
      writeMe.add("===========================================");
      writeMe.add("These are normal days.  Crime Level Averages outside of Confidence Interval are Abnormal");
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

      sc.parallelize(writeMe, 1).saveAsTextFile("DayOfWeekConfidenceIntervalNotFullMoonOutside");
   }

    Tuple2<String, Integer> dateTransform(String[] split) {
      LocalDate day = Utils.getLocalDate(split[CrimesHelper.DATE_INDEX]);
      String crime = DayOfWeekConfidenceIntervalNotFullMoonOutside.getType(split);

      if (Utils.isValidString(crime) && day != null) {
         return new Tuple2<>(day.toString() + "$" + crime, 1);
      }

      return null;
   }

    void breakUpFlatCount(Map<String, Integer> flatCount) {
      flatCount.entrySet().forEach(entry -> {
         String[] daycrime = entry.getKey().split("\\$");
         collectData(daycrime[0], daycrime[1], entry.getValue());
      });
   }

   private void collectData(String stDay, String crime, int count) {
      LocalDate day = LocalDate.parse(stDay);

      if (Utils.isValidString(crime) && day != null) {
         Map<String, Integer> dailyCrime = getDailyCrime(day);
         addCrime(dailyCrime, crime, count);
      }
   }

   private void addCrime(Map<String, Integer> dailyCrime, String crime, int more) {
      Integer count = dailyCrime.get(crime);

      if (count == null) {
         dailyCrime.put(crime, more);
      }
      else {
         dailyCrime.put(crime, count + more);
      }
   }

   private Map<String, Integer> getDailyCrime(LocalDate day) {
      Map<String, Integer> rtn = this.crimes.get(day);

      if (rtn == null) {
         rtn = new HashMap<>();
         this.crimes.put(day, rtn);
      }

      return rtn;
   }

    void buildVariance(Map.Entry<LocalDate, Map<String, Integer>> localDateMapEntry) {
      String day = localDateMapEntry.getKey().getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.US);
      Map<String, Integer> todaysCrimes = localDateMapEntry.getValue();
      CrimesToday ct = getDaysStats(day);

      todaysCrimes.entrySet().forEach(entry -> ct.addVariance(entry.getKey(), entry.getValue()));

   }

    void buildBaseStats(Map.Entry<LocalDate, Map<String, Integer>> localDateMapEntry) {
      String day = localDateMapEntry.getKey().getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.US);
      Map<String, Integer> todaysCrimes = localDateMapEntry.getValue();
      CrimesToday ct = getDaysStats(day);
      ct.numberOfThisWeekDay++;

      todaysCrimes.entrySet().forEach(entry -> ct.addCrime(entry.getKey(), entry.getValue()));

   }

   private CrimesToday getDaysStats(String day) {
      CrimesToday rtn = this.stats.get(day);

      if (rtn == null) {
         rtn = new CrimesToday(day);
         this.stats.put(day, rtn);
      }

      return rtn;
   }

   @Override void compileFullMoons(List<LocalDate> dates) {
      for (LocalDate date : dates) {
         if (!fullMoonDates.contains(date))
            fullMoonDates.add(date);
      }
   }

    static class CrimesToday implements Serializable {

      private static final float zee = 1.960f; // for ConfIntv of .95

      String weekday;
      int numberOfThisWeekDay = 0;
      Map<String, Integer> dailyCrimeCount = new HashMap<>();
      Map<String, Float> dailyCrimeAve = new HashMap<>();
      Map<String, Float> dailyCrimeVariance = new HashMap<>();
      Map<String, Float> dailyCrimeStd = new HashMap<>();
      Map<String, Float> dailyCrimeConfidenceInterval = new HashMap<>();

      CrimesToday(String day) {
         this.weekday = day;
      }

      void addCrime(String crime, Integer count) {
         Integer totCount = this.dailyCrimeCount.get(crime);

         if (totCount == null) {
            this.dailyCrimeCount.put(crime, 1);
         }
         else {
            this.dailyCrimeCount.put(crime, totCount + count);
         }
      }

      void calcAverages() {
         for (Map.Entry<String, Integer> entry : this.dailyCrimeCount.entrySet()) {
            float ave = (float) entry.getValue() / (float) numberOfThisWeekDay;
            this.dailyCrimeAve.put(entry.getKey(), ave);
         }
      }

      void addVariance(String crime, Integer count) {
         float ave = this.dailyCrimeAve.getOrDefault(crime, 0f);
         float variance = (float) Math.pow((count - ave), 2) / (float) this.numberOfThisWeekDay;

         Float totVar = this.dailyCrimeVariance.get(crime);

         if (totVar == null) {
            this.dailyCrimeVariance.put(crime, variance);
         }
         else {
            this.dailyCrimeVariance.put(crime, totVar + variance);
         }
      }

      void calcConfIntv() {
         for (Map.Entry<String, Float> entry : this.dailyCrimeVariance.entrySet()) {
            float stdDev = (float) Math.sqrt(entry.getValue());
            this.dailyCrimeStd.put(entry.getKey(), stdDev);

            float confIntv = (float) (zee * stdDev / Math.sqrt(this.numberOfThisWeekDay));
            this.dailyCrimeConfidenceInterval.put(entry.getKey(), confIntv);
         }
      }
   }
}
