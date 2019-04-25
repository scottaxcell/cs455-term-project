package cs455.project.drivers;

import cs455.project.crimes.CrimesHelper;
import cs455.project.moons.MoonPhase;
import cs455.project.moons.MoonsHelper;
import cs455.project.utils.Utils;
import org.apache.spark.broadcast.Broadcast;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Driver {

   List<LocalDate> fullMoonDates = new ArrayList<>();
   Map<String, Integer> crimeTypeToCount = new HashMap<>();

   void collateCrimes(List<String> crimes) {
      for (String crime : crimes)
         incrementCrimeCount(crime);
   }

   void incrementCrimeCount(String crime) {
      int count = crimeTypeToCount.containsKey(crime) ? crimeTypeToCount.get(crime) : 0;
      crimeTypeToCount.put(crime, count + 1);
   }

   static boolean crimeOccurredOnFullMoon(Broadcast<List<LocalDate>> fullMoonDates, String[] split) {
      LocalDate date = Utils.getLocalDate(split[CrimesHelper.DATE_INDEX]);
      if (date == null)
         return false;
      return fullMoonDates.value().contains(date);
   }

   static String getType(String[] split) {
      return split[CrimesHelper.PRIMARY_TYPE_INDEX];
   }

   /**
    * Adds the dates of the full moon, the next days, and previous days to full moon list
    *
    * @param dates
    */
   void compileFullMoons(List<LocalDate> dates) {
      for (LocalDate date : dates) {
         if (!fullMoonDates.contains(date))
            fullMoonDates.add(date);
         LocalDate nextDate = date.plusDays(1);
         if (!fullMoonDates.contains(nextDate))
            fullMoonDates.add(nextDate);
         LocalDate previousDate = date.plusDays(-1);
         if (!fullMoonDates.contains(previousDate))
            fullMoonDates.add(previousDate);
      }
   }

   static boolean isFullMoon(String[] split) {
      int phaseId = Integer.parseInt(split[MoonsHelper.PHASE_ID_INDEX]);
      return phaseId == MoonPhase.FULL_MOON.getPhaseId();
   }

   static LocalDate getDate(String[] split) {
      String date = split[MoonsHelper.DATE_INDEX];
      return Utils.getLocalDate(date);
   }
}
