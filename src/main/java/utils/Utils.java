package utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Utils {
	
	
	public static String getCurrentDateTimeasStr() {
		
		 // Get the current date and time
        LocalDateTime now = LocalDateTime.now();

        // Define the desired format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

        // Format the date and time as a string
        String formattedDateTime = now.format(formatter);

        // Print the result
        //System.out.println("Current Date and Time: " + formattedDateTime);
        return formattedDateTime;
	}

	
	/**
	 * latitude typically ranges from -90 to 90 degrees, but if you want to map it to a range of 0 to 360, 
	 * you can use a simple transformation. 
	 */
	
	public static double convertLatitudeTo360(double latitude) {
        // Ensure latitude is within the valid range of -90 to 90
        if (latitude < -90 || latitude > 90) {
            throw new IllegalArgumentException("Latitude must be between -90 and 90 degrees.");
        }
        // Map latitude to the range 0 to 360
        return latitude + 90; // Shift by 90 to make the range 0 to 180
    }
	
	public static double getDoubleFromBigDecimal ( java.math.BigDecimal bigDecimal  ) {
		
		try {
			Double doubleValue = bigDecimal.doubleValue();
			return doubleValue;
		} catch(Exception e) {
			return (double)0.0;
		}
	}
	
	public static float getFloatFromBigDecimal( java.math.BigDecimal bigDecimal ) {
		
		try {
			Float floatValue = bigDecimal.floatValue();
			return floatValue;
		} catch(Exception e) {
			  return (float)0.0;
		}
	}
	
	// https://www.baeldung.com/cs/haversine-formula
	
	public static double haversine(double val) {
	    return Math.pow(Math.sin(val / 2.0), 2.0);
	}
	
	public static double calculateHaversineDistanceNauticalMiles(double startLatitudeDegrees, double startLongitudeDegrees, 
			double endLatitudeDegrees, double endLongitudeDegrees) {

	    double dLat = Math.toRadians((endLatitudeDegrees - startLatitudeDegrees));
	    double dLong = Math.toRadians((endLongitudeDegrees - startLongitudeDegrees));

	    double startLatitudeRadians = Math.toRadians(startLatitudeDegrees);
	    double endLatitudeRadians = Math.toRadians(endLatitudeDegrees);

	    double a = haversine(dLat) + Math.cos(startLatitudeRadians) * Math.cos(endLatitudeRadians) * haversine(dLong);
	    double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));

	    return Constants.EARTH_RADIUS_kilometers * c * Constants.Kilometers_to_Nautical_Miles ;
	}

}
