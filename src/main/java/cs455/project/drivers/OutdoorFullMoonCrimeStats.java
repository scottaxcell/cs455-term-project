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

/**
 * Collects outdoor crime statistics on days of a full moon
 * - prints total number of crimes
 * - prints percentage of crime per type
 */
public class OutdoorFullMoonCrimeStats extends Driver {

    public static void main(String[] args) {
        new OutdoorFullMoonCrimeStats().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Outdoor Full Moon Crime Stats");
        //        SparkConf conf = new SparkConf().setMaster("local").setAppName("Outdoor Full Moon Crime Stats");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(Constants.HDFS_MOONS_DIR);
        //        JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs455TermProject/data/moon-phases-*.csv");
        List<LocalDate> fullMoonDates = textFile
                .filter(MoonsHelper::isValidEntry)
                .map(Utils::splitCommaDelimitedString)
                .filter(OutdoorFullMoonCrimeStats::isFullMoon)
                .map(OutdoorFullMoonCrimeStats::getDate)
                .filter(Objects::nonNull)
                .collect();

        compileFullMoons(fullMoonDates);

        Broadcast<List<LocalDate>> fullMoonDatesBroadcast = sc.broadcast(this.fullMoonDates);
        List<String> outdoorLocations = Stream.of(CrimesHelper.OUTDOOR_LOCATIONS).collect(Collectors.toList());
        Broadcast<List<String>> outdoorLocationsBroadcast = sc.broadcast(outdoorLocations);

        textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
        //        textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs455TermProject/data/chicagoCrimes2001ToPresent.csv");
        List<String> crimes = textFile
                .filter(CrimesHelper::isValidEntry)
                .map(Utils::splitCommaDelimitedString)
                .filter(split -> crimeOccurredOnFullMoon(fullMoonDatesBroadcast, split))
                .filter(split -> crimeOccurredOutdoors(outdoorLocationsBroadcast, split))
                .map(OutdoorFullMoonCrimeStats::getType)
                .filter(Utils::isValidString)
                .collect();

        collateCrimes(crimes);

        saveStatisticsToFile(sc);
    }

    private static boolean crimeOccurredOutdoors(Broadcast<List<String>> outdoorLocations, String[] split) {
        return outdoorLocations.value().contains(split[CrimesHelper.LOCATION_DESCRIPTION_INDEX]);
    }

    private void saveStatisticsToFile(JavaSparkContext sc) {
        int totalNumCrimes = (int) crimeTypeToCount.values().stream().collect(Collectors.summarizingInt(i -> i)).getSum();

        List<String> writeMe = new ArrayList<>();
        writeMe.add("Outdoor Full Moon Crime Statistics");
        writeMe.add("==================================");
        writeMe.add(String.format("Total # crimes: %d", totalNumCrimes));
        writeMe.add("Crime Percentage By Type");
        writeMe.add("------------------------");
        crimeTypeToCount.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(e -> {
            double percentage = Utils.calculatePercentageOfTotal(e.getValue(), totalNumCrimes);
            writeMe.add(String.format("%s (%d): %.2f", e.getKey(), e.getValue(), percentage));
        });

        sc.parallelize(writeMe, 1).saveAsTextFile("OutdoorFullMoonCrimeStats");
    }
}
