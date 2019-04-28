package cs455.project.drivers;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import cs455.project.crimes.CrimesHelper;
import cs455.project.moons.MoonPhase;
import cs455.project.moons.MoonsHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;

public class DayOfWeekCrimeStatsWithFullMoon {
	private List<LocalDate> fullMoonDates = new ArrayList<>();
	private Map<String, Map<String, Integer>> dayToCrimeTypeWithCount = new HashMap();

    public static void main(String[] args) {
        new DayOfWeekCrimeStatsWithFullMoon().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Day of Week Crime With Full Moon Stats");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> textFile = sc.textFile(Constants.HDFS_MOONS_DIR);
//      JavaRDD<String> textFile = sc.textFile("/s/chopin/a/grad/sgaxcell/cs455TermProject/data/moon-phases-*.csv");
        List<LocalDate> fullMoonDates = textFile
        		.filter(MoonsHelper::isValidEntry)
        		.map(Utils::splitCommaDelimitedString)
        		.filter(DayOfWeekCrimeStatsWithFullMoon::isFullMoon)
        		.map(DayOfWeekCrimeStatsWithFullMoon::getDate)
        		.filter(Objects::nonNull)
        		.collect();
        
        compileFullMoons(fullMoonDates);
        
        Broadcast<List<LocalDate>> fullMoonDatesBroadcast = sc.broadcast(this.fullMoonDates);
        
        textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);

        for (String s : Constants.DAYS_OF_WEEK) {
        	List<String> crimes = textFile
                    .filter(CrimesHelper::isValidEntry)
                    .map(Utils::splitCommaDelimitedString)
                    .filter(day -> getWeekdayName(day).equals(s))
                    .filter(split -> crimeOccurredOnFullMoon(fullMoonDatesBroadcast, split))
                    .map(DayOfWeekCrimeStatsWithFullMoon::getType)
                    .filter(Utils::isValidString)
                    .collect();
        	
        	Map<String, Integer> crimeTypeToCountAgain = collateCrimesToList(crimes);
        	dayToCrimeTypeWithCount.put(s, crimeTypeToCountAgain);
        }

        saveStatisticsToFile(sc);
    }

    private void saveStatisticsToFile(JavaSparkContext sc) {

        List<String> writeMe = new ArrayList<>();
        writeMe.add("Day of Week Crime With Full Moon Statistics");
        writeMe.add("===========================================");
        //writeMe.add(String.format("Total # crimes: %d", totalNumCrimes));
        writeMe.add("Crime Percentage By Type Only on Full Moons on Corresponding Day of Week");
        writeMe.add("------------------------------------------------------------------------");
        
        for (int i = 0; i < Constants.DAYS_OF_WEEK.length; i++) {
        	if (!Constants.DAYS_OF_WEEK[i].isEmpty()) {
        		String day = Constants.DAYS_OF_WEEK[i];
        		Map<String, Integer> entry = dayToCrimeTypeWithCount.get(Constants.DAYS_OF_WEEK[i]);
	        	writeMe.add("============");
	        	writeMe.add(Constants.DAYS_OF_WEEK[i]);
	        	writeMe.add("============");
	        	
	        	int crimeTotal = (int) entry.values().stream()
	                    .collect(Collectors.summarizingInt(j -> j)).getSum();
	        	entry.entrySet().stream()
	        		.sorted(Map.Entry.comparingByValue())
	        		.forEach(e -> {
	        		double percentage = Utils.calculatePercentageOfTotal(e.getValue(), crimeTotal);
	                writeMe.add(String.format("%s (%d): %.2f", e.getKey(), e.getValue(), percentage));
	        	});
	        	writeMe.add("\n");
	            writeMe.add("Day Of Week,Type,Number,% of Total");
	            entry.entrySet().stream()
        			.sorted(Map.Entry.comparingByValue())
        			.forEach(e -> {
        		double percentage = Utils.calculatePercentageOfTotal(e.getValue(), crimeTotal);
                writeMe.add(String.format("%s,%s,%d,%.2f", day, e.getKey(), e.getValue(), percentage));
        	});
	        	
        	}
        }
        
        sc.parallelize(writeMe, 1)
            .saveAsTextFile("DayOfWeekCrimeStatsWithFullMoon");
    }

    private Map<String, Integer> collateCrimesToList(List<String> crimes) {
    	Map<String, Integer> crimeTypeToCountNow = new HashMap<>();
        for (String crime : crimes) {
        	int count = crimeTypeToCountNow.containsKey(crime) ? crimeTypeToCountNow.get(crime) : 0;
        	crimeTypeToCountNow.put(crime, count + 1);
        }
        return crimeTypeToCountNow;
    }

    private static String getType(String[] split) {
        return split[CrimesHelper.PRIMARY_TYPE_INDEX];
    }

    private static String getWeekdayName(String[] split) {
    	String date = split[CrimesHelper.DATE_INDEX];
    	return Utils.getDayOfWeek(date);
    }
    
    private static boolean isFullMoon(String[] split) {
        int phaseId = Integer.parseInt(split[MoonsHelper.PHASE_ID_INDEX]);
        return phaseId == MoonPhase.FULL_MOON.getPhaseId();
    }
    
    private static LocalDate getDate(String[] split) {
        String date = split[MoonsHelper.DATE_INDEX];
        return Utils.getLocalDate(date);
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
        }
    }
    
    private static boolean crimeOccurredOnFullMoon(Broadcast<List<LocalDate>> fullMoonDates, String[] split) {
        LocalDate date = Utils.getLocalDate(split[CrimesHelper.DATE_INDEX]);
        if (date == null)
            return false;
        return fullMoonDates.value().contains(date);
    }
}
