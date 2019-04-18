package cs455.project.moons;

import cs455.project.utils.Utils;

public class MoonsHelper {
    public static final int NUM_FIELDS = 8;
    public static final int DATE_INDEX = 0;
    public static final int TIME_INDEX = 1;
    public static final int PHASE_INDEX = 2;
    public static final int PHASE_ID_INDEX = 3;
    public static final int DATE_TIME_INDEX = 4;
    public static final int TIME_STAMP_INDEX = 5;
    public static final int FRIENDLY_DATE_INDEX_0 = 6;  // friendlydate field splits into two fields due to hidden comma
    public static final int FRIENDLY_DATE_INDEX_1 = 7;

    private MoonsHelper() {
    }

    public static boolean isValidEntry(String entry) {
        if (!Utils.isValidString(entry))
            return false;
        String[] split = Utils.splitCommaDelimitedString(entry);
        if (split.length != NUM_FIELDS)
            return false;
        if (split[0].equalsIgnoreCase("date")) // header
            return false;
        return true;
    }
}
