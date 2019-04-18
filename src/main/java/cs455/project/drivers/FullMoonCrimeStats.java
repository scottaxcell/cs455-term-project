package cs455.project.drivers;

import cs455.project.crimes.CrimesHelper;
import cs455.project.moons.MoonPhase;
import cs455.project.moons.MoonsHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Collects crime statistics on days of a full moon
 * - prints total number of crimes
 * - prints percentage of crime per type
 */
public class FullMoonCrimeStats {
    private List<LocalDate> fullMoonDates = new ArrayList<>();
    private Map<String, Integer> crimeTypeToCount = new HashMap<>();

    public static void main(String[] args) {
        new FullMoonCrimeStats().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Full Moon Crime Stats");
//        SparkConf conf = new SparkConf().setMaster("local").setAppName("Full Moon Crime Stats");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(Constants.HDFS_MOONS_DIR);
//        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs455TermProject/data/moon-phases-*.csv");
        List<LocalDate> fullMoonDates = textFile
            .filter(MoonsHelper::isValidEntry)
            .map(Utils::splitCommaDelimitedString)
            .filter(FullMoonCrimeStats::isFullMoon)
            .map(FullMoonCrimeStats::getDate)
            .filter(Objects::nonNull)
            .collect();

        compileFullMoons(fullMoonDates);

        Broadcast<List<LocalDate>> fullMoonDatesBroadcast = sc.broadcast(this.fullMoonDates);

        textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
//        textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs455TermProject/data/chicagoCrimes2001ToPresent.csv");
        List<String> crimes = textFile
            .filter(CrimesHelper::isValidEntry)
            .map(Utils::splitCommaDelimitedString)
            .filter(split -> crimeOccurredOnFullMoon(fullMoonDatesBroadcast, split))
            .map(FullMoonCrimeStats::getType)
            .filter(Utils::isValidString)
            .collect();

        collateCrimes(crimes);

        saveStatisticsToFile(sc);
    }

    private void saveStatisticsToFile(JavaSparkContext sc) {
        int totalNumCrimes = (int) crimeTypeToCount.values().stream()
            .collect(Collectors.summarizingInt(i -> i)).getSum();

        List<String> writeMe = new ArrayList<>();
        writeMe.add("Full Moon Crime Statistics");
        writeMe.add("==========================");
        writeMe.add(String.format("Total # crimes: %d", totalNumCrimes));
        writeMe.add("Crime Percentage By Type");
        writeMe.add("------------------------");
        crimeTypeToCount.entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .forEach(e -> {
                double percentage = Utils.calculatePercentageOfTotal(e.getValue(), totalNumCrimes);
                writeMe.add(String.format("%s (%d): %.2f", e.getKey(), e.getValue(), percentage));
            });

        sc.parallelize(writeMe, 1)
            .saveAsTextFile("FullMoonCrimeStats");
    }

    private void collateCrimes(List<String> crimes) {
        for (String crime : crimes)
            incrementCrimeCount(crime);
    }

    private void incrementCrimeCount(String crime) {
        int count = crimeTypeToCount.containsKey(crime) ? crimeTypeToCount.get(crime) : 0;
        crimeTypeToCount.put(crime, count + 1);
    }

    private static boolean crimeOccurredOnFullMoon(Broadcast<List<LocalDate>> fullMoonDates, String[] split) {
        LocalDate date = Utils.getLocalDate(split[CrimesHelper.DATE_INDEX]);
        if (date == null)
            return false;
        return fullMoonDates.value().contains(date);
    }

    private static String getType(String[] split) {
        return split[CrimesHelper.PRIMARY_TYPE_INDEX];
    }

    /**
     * Adds the dates of the full moon, the next days, and previous days to full moon list
     *
     * @param dates
     */
    private void compileFullMoons(List<LocalDate> dates) {
        for (LocalDate date : dates) {
            if (!fullMoonDates.contains(date))
                fullMoonDates.add(date);
//            LocalDate nextDate = date.plusDays(1);
//            if (!fullMoonDates.contains(nextDate))
//                fullMoonDates.add(nextDate);
//            LocalDate previousDate = date.plusDays(-1);
//            if (!fullMoonDates.contains(previousDate))
//                fullMoonDates.add(previousDate);
        }
    }

    private static boolean isFullMoon(String[] split) {
        int phaseId = Integer.parseInt(split[MoonsHelper.PHASE_ID_INDEX]);
        return phaseId == MoonPhase.FULL_MOON.getPhaseId();
    }

    private static LocalDate getDate(String[] split) {
        String date = split[MoonsHelper.DATE_INDEX];
        return Utils.getLocalDate(date);
    }
}
