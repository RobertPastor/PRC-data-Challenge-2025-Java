package utils;

public class Utils {
	
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
