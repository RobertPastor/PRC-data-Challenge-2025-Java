package utils;

public class Utils {
	
	// https://www.baeldung.com/cs/haversine-formula
	
	private static double EARTH_RADIUS_kilometers = 6371.0;
	private static double Kilometers_to_Nautical_Miles = 0.6213711922 ;
	
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

	    return EARTH_RADIUS_kilometers * c * Kilometers_to_Nautical_Miles ;
	}

}
