package cs455.project.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("HH:mm:ss");
    private static boolean DEBUG = true;

    public static void out(Object o) {
        System.out.print(o);
    }

    public static void info(Object o) {
        System.out.println("\nINFO: " + o);
    }

    public static void debug(Object o) {
        if (DEBUG)
            System.out.println(String.format("DEBUG: [%s] %s", SIMPLE_DATE_FORMAT.format(new Date()), o));
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
}
