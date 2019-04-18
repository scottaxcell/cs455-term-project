package cs455.project.utils;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
    private static final DateTimeFormatter DATASET_DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    private static Pattern DATASET_DATE_PATTERN = Pattern.compile("\\d{2}/\\d{2}/\\d{4}");

    public static void out(Object o) {
        System.out.print(o);
    }

    public static void info(Object o) {
        System.out.println("\nINFO: " + o);
    }

    public static void debug(Object o) {
        System.out.println(String.format("DEBUG: [%s] %s", TIME_FORMAT.format(new Date()), o));
    }

    public static void error(Object o) {
        System.err.println("\nERROR: " + o);
    }

    public static boolean isValidString(String str) {
        return str != null && !str.isEmpty() && !str.trim().isEmpty();
    }

    public static String[] splitCommaDelimitedString(String s) {
        return s.split(Constants.COMMA_STR);
    }

    public static LocalDate getLocalDate(String s) {
        Matcher matcher = DATASET_DATE_PATTERN.matcher(s);
        if (matcher.find())
            return LocalDate.parse(matcher.group(), DATASET_DATE_FORMATTER);
        return null;
    }

    public static double calculatePercentageOfTotal(int value, int total) {
        return value * 100d / total;
    }
}
