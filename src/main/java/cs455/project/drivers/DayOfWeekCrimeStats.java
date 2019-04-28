package cs455.project.drivers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import cs455.project.crimes.CrimesHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;

public class DayOfWeekCrimeStats {
	private Map<String, Map<String, Integer>> dayToCrimeTypeWithCount = new HashMap();

    public static void main(String[] args) {
        new DayOfWeekCrimeStats().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Day of Week Crime Stats");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(Constants.HDFS_CRIMES_DIR);
        
        for (String s : Constants.DAYS_OF_WEEK) {
        	List<String> crimes = textFile
                    .filter(CrimesHelper::isValidEntry)
                    .map(Utils::splitCommaDelimitedString)
                    .filter(day -> getWeekdayName(day).equalsIgnoreCase(s))
                    .map(DayOfWeekCrimeStats::getType)
                    .filter(Utils::isValidString)
                    .collect();
        	
        	Map<String, Integer> crimeTypeToCountAgain = collateCrimesToList(crimes);
        	dayToCrimeTypeWithCount.put(s, crimeTypeToCountAgain);
        }

        saveStatisticsToFile(sc);
    }

    private void saveStatisticsToFile(JavaSparkContext sc) {
    	//int totalNumCrimes = (int) crimeTypeToCount.values().stream().collect(Collectors.summarizingInt(i -> i)).getSum();
    	
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Day of Week Crime Statistics");
        writeMe.add("============================");
        //writeMe.add(String.format("Total # crimes: %d", totalNumCrimes));
        writeMe.add("Crime Percentage By Type on Corresponding Day of Week");
        writeMe.add("-----------------------------------------------------");
        
        for (int i = 0; i < Constants.DAYS_OF_WEEK.length; i++) {
        	if (!Constants.DAYS_OF_WEEK[i].isEmpty()) {
        		writeMe.add("\n");
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
            .saveAsTextFile("DayOfWeekCrimeStats");
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
    
}
