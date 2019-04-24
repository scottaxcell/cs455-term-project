package cs455.project.crimes;

import cs455.project.utils.Utils;

import java.util.stream.Stream;

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

    public static boolean isLocationOutside(String location) {
        return Stream.of(outsideLocations).anyMatch(location::equals);
    }

    public static final String[] outsideLocations = new String[]{
        "ABANDONED BUILDING",
        "AIRCRAFT",
        "AIRPORT BUILDING NON-TERMINAL - NON-SECURE AREA",
        "AIRPORT BUILDING NON-TERMINAL - SECURE AREA",
        "AIRPORT TERMINAL LOWER LEVEL - NON-SECURE AREA",
        "AIRPORT TERMINAL LOWER LEVEL - SECURE AREA",
        "AIRPORT TERMINAL MEZZANINE - NON-SECURE AREA",
        "AIRPORT TERMINAL UPPER LEVEL - NON-SECURE AREA",
        "AIRPORT TERMINAL UPPER LEVEL - SECURE AREA",
        "AIRPORT TRANSPORTATION SYSTEM (ATS)",
        "AIRPORT VENDING ESTABLISHMENT",
        "AIRPORT/AIRCRAFT",
        "ANIMAL HOSPITAL",
        "APARTMENT",
        "APPLIANCE STORE",
        "ATHLETIC CLUB",
        "AUTO",
        "AUTO / BOAT / RV DEALERSHIP",
        "BANK",
        "BANQUET HALL",
        "BAR OR TAVERN",
        "BARBER SHOP/BEAUTY SALON",
        "BARBERSHOP",
        "BASEMENT",
        "BOWLING ALLEY",
        "CAR WASH",
        "CHA APARTMENT",
        "CHA BREEZEWAY",
        "CHA ELEVATOR",
        "CHA HALLWAY",
        "CHA HALLWAY/STAIRWELL/ELEVATOR",
        "CHA LOBBY",
        "CHA STAIRWELL",
        "CHURCH",
        "CHURCH/SYNAGOGUE/PLACE OF WORSHIP",
        "CLEANERS/LAUNDROMAT",
        "CLEANING STORE",
        "CLUB",
        "COACH HOUSE",
        "COIN OPERATED MACHINE",
        "COLLEGE/UNIVERSITY RESIDENCE HALL",
        "COMMERCIAL / BUSINESS OFFICE",
        "CONVENIENCE STORE",
        "COUNTY JAIL",
        "CREDIT UNION",
        "CURRENCY EXCHANGE",
        "DAY CARE CENTER",
        "DEPARTMENT STORE",
        "DRUG STORE",
        "DUMPSTER",
        "ELEVATOR",
        "FACTORY",
        "FACTORY/MANUFACTURING BUILDING",
        "FARM",
        "FEDERAL BUILDING",
        "FIRE STATION",
        "FUNERAL PARLOR",
        "GARAGE",
        "GARAGE/AUTO REPAIR",
        "GOVERNMENT BUILDING",
        "GOVERNMENT BUILDING/PROPERTY",
        "GROCERY FOOD STORE",
        "HALLWAY",
        "HOSPITAL",
        "HOSPITAL BUILDING/GROUNDS",
        "HOTEL",
        "HOTEL/MOTEL",
        "HOUSE",
        "JAIL / LOCK-UP FACILITY",
        "LAUNDRY ROOM",
        "LIBRARY",
        "LIQUOR STORE",
        "LIVERY AUTO",
        "LIVERY STAND OFFICE",
        "MEDICAL/DENTAL OFFICE",
        "MOTEL",
        "MOVIE HOUSE/THEATER",
        "NURSING HOME",
        "NURSING HOME/RETIREMENT HOME",
        "OFFICE",
        "OTHER",
        "PAWN SHOP",
        "POLICE FACILITY/VEH PARKING LOT",
        "POOL ROOM",
        "POOLROOM",
        "PUBLIC GRAMMAR SCHOOL",
        "PUBLIC HIGH SCHOOL",
        "RESIDENCE",
        "RESTAURANT",
        "RETAIL STORE",
        "ROOMING HOUSE",
        "SAVINGS AND LOAN",
        "SMALL RETAIL STORE",
        "SPORTS ARENA/STADIUM",
        "STAIRWELL",
        "TAVERN",
        "TAVERN/LIQUOR STORE",
        "VESTIBULE",
        "WAREHOUSE",
        "YMCA"
    };

    public static boolean isLocationInside(String location) {
        return Stream.of(insideLocations).anyMatch(location::equals);
    }

    public static final String[] insideLocations = new String[]{
        "\"CTA \"\"L\"\" PLATFORM\"",
        "\"CTA \"\"L\"\" TRAIN\"",
        "AIRPORT EXTERIOR - NON-SECURE AREA",
        "AIRPORT EXTERIOR - SECURE AREA",
        "AIRPORT PARKING LOT",
        "ALLEY",
        "ATM (AUTOMATIC TELLER MACHINE)",
        "BOAT/WATERCRAFT",
        "BRIDGE",
        "CEMETARY",
        "CHA GROUNDS",
        "CHA PARKING LOT",
        "CHA PARKING LOT/GROUNDS",
        "CHA PLAY LOT",
        "CHURCH PROPERTY",
        "COLLEGE/UNIVERSITY GROUNDS",
        "CONSTRUCTION SITE",
        "CTA BUS",
        "CTA BUS STOP",
        "CTA GARAGE / OTHER PROPERTY",
        "CTA PLATFORM",
        "CTA PROPERTY",
        "CTA STATION",
        "CTA TRACKS - RIGHT OF WAY",
        "CTA TRAIN",
        "DELIVERY TRUCK",
        "DRIVEWAY",
        "DRIVEWAY - RESIDENTIAL",
        "EXPRESSWAY EMBANKMENT",
        "FOREST PRESERVE",
        "GANGWAY",
        "GAS STATION",
        "GAS STATION DRIVE/PROP.",
        "HIGHWAY/EXPRESSWAY",
        "HORSE STABLE",
        "HOSPITAL BUILDING/GROUNDS",
        "JUNK YARD/GARBAGE DUMP",
        "KENNEL",
        "LAGOON",
        "LAKE",
        "LAKEFRONT/WATERFRONT/RIVERBANK",
        "LOADING DOCK",
        "NEWSSTAND",
        "OTHER",
        "OTHER COMMERCIAL TRANSPORTATION",
        "OTHER RAILROAD PROP / TRAIN DEPOT",
        "PARK PROPERTY",
        "PARKING LOT",
        "PARKING LOT/GARAGE(NON.RESID.)",
        "POLICE FACILITY/VEH PARKING LOT",
        "PORCH",
        "PRAIRIE",
        "RAILROAD PROPERTY",
        "RESIDENCE PORCH/HALLWAY",
        "RESIDENCE-GARAGE",
        "RESIDENTIAL YARD (FRONT/BACK)",
        "RIVER",
        "RIVER BANK",
        "SCHOOL YARD",
        "SEWER",
        "SIDEWALK",
        "STREET",
        "TAXI CAB",
        "TAXICAB",
        "TRAILER",
        "TRUCK",
        "TRUCKING TERMINAL",
        "VACANT LOT",
        "VACANT LOT/LAND",
        "VEHICLE - DELIVERY TRUCK",
        "VEHICLE - OTHER RIDE SERVICE",
        "VEHICLE NON-COMMERCIAL",
        "VEHICLE-COMMERCIAL",
        "VEHICLE-COMMERCIAL - ENTERTAINMENT/PARTY BUS",
        "VEHICLE-COMMERCIAL - TROLLEY BUS",
        "WOODED AREA",
        "YARD"
    };
}
