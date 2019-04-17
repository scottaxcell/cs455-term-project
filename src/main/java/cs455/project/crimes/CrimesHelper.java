package cs455.project.crimes;

import cs455.project.utils.Utils;

public class CrimesHelper {
    public static final int NUM_FIELDS = 23;
    public static final int DATE_INDEX = 2;
    public static final int PRIMARY_TYPE_INDEX = 5;
    public static final int LOCATION_DESCRIPTION_INDEX = 7;
    public static final int YEAR_INDEX = 17;

    private CrimesHelper() {
    }

    public static boolean isValidEntry(String row) {
        if (!Utils.isValidString(row))
            return false;
        String[] split = Utils.splitCommaDelimitedString(row);
        if (split.length != NUM_FIELDS)
            return false;
        if (split[0].equalsIgnoreCase("ID")) // header
            return false;
        return true;
    }
}
