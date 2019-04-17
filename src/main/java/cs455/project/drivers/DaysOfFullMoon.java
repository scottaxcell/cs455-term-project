package cs455.project.drivers;

import cs455.project.moons.MoonPhase;
import cs455.project.moons.MoonsHelper;
import cs455.project.utils.Constants;
import cs455.project.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class DaysOfFullMoon {
    public static void main(String[] args) {
        new DaysOfFullMoon().run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Days Of Full Moon");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(Constants.HDFS_MOONS_DIR);
        List<String> fullMoonDays = textFile
                .filter(DaysOfFullMoon::isFullMoonDay)
                .map(DaysOfFullMoon::getMoonData)
                .collect();

        saveFullMoonsToFile(sc, fullMoonDays);
    }

    private static boolean isFullMoonDay(String s) {
        if (!MoonsHelper.isValidEntry(s))
            return false;
        String[] split = Utils.splitCommaDelimitedString(s);
        int phaseId = Integer.parseInt(split[MoonsHelper.PHASE_ID_INDEX]);
        return MoonPhase.FULL_MOON.getPhaseId() == phaseId;
    }

    private static String getMoonData(String s) {
        String[] split = Utils.splitCommaDelimitedString(s);
        String date = split[MoonsHelper.DATE_INDEX];
        String friendlyDate = split[MoonsHelper.FRIENDLY_DATE_INDEX];
        String phase = split[MoonsHelper.PHASE_INDEX];
        String time = split[MoonsHelper.TIME_INDEX];
        return String.format("%s : %s : %s : %s", date, friendlyDate, time, phase);
    }

    private static void saveFullMoonsToFile(JavaSparkContext sc, List<String> fullMoons) {
        sc.parallelize(fullMoons, 1)
                .saveAsTextFile("daysOfFullMoon");
    }
}
