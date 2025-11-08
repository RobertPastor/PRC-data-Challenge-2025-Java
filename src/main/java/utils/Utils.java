package utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;

import aircrafts.AircraftsDataTable;


class CustomAltitudeMetersRangeException extends Exception {
	/**
	 * serial generated ID
	 */
	private static final long serialVersionUID = -1346311432020834637L;

	public CustomAltitudeMetersRangeException(String message) {
		super(message);
	}
}

class CustomExceptionNotYetImplemented extends Exception {
	/**
	 * serial generated ID
	 */
	private static final long serialVersionUID = -1346311432020834637L;

	public CustomExceptionNotYetImplemented(String message) {
		super(message);
	}
}

public class Utils {
	
	private static final Logger logger = Logger.getLogger(AircraftsDataTable.class.getName());

	/**
	 * 
	 * @param mach
	 * @param altitudeFeet
	 * @return
	 * @throws CustomAltitudeMetersRangeException 
	 * @throws CustomExceptionNotYetImplemented 
	 */
	public static double convertMachToCASknots_Not_Working (final double mach, final double altitudeFeet) throws CustomAltitudeMetersRangeException, CustomExceptionNotYetImplemented {
        
        // Convert altitude to meters
        double altitudeMeters = altitudeFeet * Constants.feet_to_meters;
        
        double tasKnots = Utils.convertMachToTASknots(mach, altitudeFeet);
        
        final double seaLevelPressurePascals = 101325; // Pressure at sea level in Pascals
        final double temperatureLapseRateKelvinMeters = 0.0065; // Temperature lapse rate in K/m
        final double seaLevelTemperatureKelvin = 288.15; // Standard temperature at sea level in Kelvin
        
        double temperatureKelvin = 0.0;
        double pressurePascals = 0.0;
        
        if ( altitudeMeters <= 11000.0) {
        	// using lapse rate
           	temperatureKelvin = Constants.SEA_LEVEL_TEMPERATURE_KELVIN + 
        			(Constants.TEMPERATURE_LAPSE_RATE_DEG_CELSIUS_PER_FEET * altitudeFeet);
        	
           	// Calculate pressure using the barometric formula
            double exponent = (Constants.gravitationalAccelerationMetersSecondsSquare * Constants.MolarMassAirKilogramsPerMole) 
            		/ (Constants.gasConstantForAirJoulePerMoleKelvins * temperatureLapseRateKelvinMeters);
            
            pressurePascals = seaLevelPressurePascals * Math.pow(
                    (seaLevelTemperatureKelvin - temperatureLapseRateKelvinMeters * altitudeMeters) / seaLevelTemperatureKelvin,
                    exponent);
            
			// g = gravitational acceleration (m/s²)
            double g = Constants.gravitationalAccelerationMetersSecondsSquare;
            //M = molecular mass of air (kg/mol)
            double M = Constants.MolarMassAirKilogramsPerMole;
            //R = specific gas constant (J/(kg·m·K))
            double R = Constants.gasConstantForAirJoulePerMoleKelvins;
            // L = lapse rate (temperature change per unit altitude)
            double L = Constants.TEMPERATURE_LAPSE_RATE_KELVIN_PER_METER;
			exponent = (g * M / (R * L));
			
			// T₀ = temperature at sea level (°C). 
			// h = altitude above sea level (m)
			double h = altitudeMeters;
			
			// https://www.mide.com/air-pressure-at-altitude-calculator
			double termToElevateToPowerOf = (1 - ((L * h) / (Constants.SEA_LEVEL_TEMPERATURE_KELVIN)));
			pressurePascals = seaLevelPressurePascals * Math.pow(termToElevateToPowerOf , exponent );
			
			//p = P₀ × (1 - (L ⋅ h) / (T₀)) ^ (g ⋅ M / (R ⋅ L)), where:
            
            // Barometric formula
            //pressurePascals = seaLevelPressurePascals * Math.pow(
             //       1 - (temperatureLapseRate * altitudeMeters) / seaLevelTemperatureKelvin,
             //       (gravitationalAccelerationMetersSecondsSquare / (gasConstantForAir * temperatureLapseRate)));

            logger.info("pressure = " + pressurePascals + " at altitude = " + altitudeMeters + " meters");
 
        } else {
        	if ( ( altitudeMeters > 11000.0 ) && ( altitudeMeters <= 20000.0) ) {
				// normal conversion of degrees celsius to Kelvin
        		// constant temperature until 20 kilometers
				temperatureKelvin = -56.5 + Constants.DEGREES_CELSIUS_TO_KELVIN ;
				
				throw new CustomExceptionNotYetImplemented ( "Compute pressure above 11 kilometers not yet implemented");
        	}else {
				// raise an exception
				throw new CustomAltitudeMetersRangeException("altitude meters above 20.000 meters");
			}
        }
         
        logger.info("Pressure = " + pressurePascals + " pascals - at altitude = " + altitudeFeet + " feet");

        double tasMetersSeconds = tasKnots * Constants.KnotsToMetersSeconds;
        // Convert TAS to CAS using simplified approximation
        double casMetersSeconds = tasMetersSeconds / Math.sqrt( pressurePascals / Constants.seaLevelPressurePascals);

        // Convert CAS from m/s to knots (1 m/s = 1.94384 knots)
        double CASknots =  casMetersSeconds * Constants.MetersSecondsToKnots;
        logger.info("At mach = " + mach + " , and altitude = " + altitudeFeet + " fett, CAS knots = " + CASknots);
        return CASknots;
    }
	
	/**
	 * 
	 * @param mach
	 * @param altitude_feet
	 * @return
	 * @throws CustomAltitudeMetersRangeException 
	 */
	public static double convertMachToTASknots( final double mach , final double altitudeFeet ) throws CustomAltitudeMetersRangeException {
		
		//At Mach 0.85 and altitude 35000 feet, the TAS is 484.25 knots.
		
		// Convert altitude from feet to meters
        double altitudeMeters = altitudeFeet * Constants.feet_to_meters;
        
        // conversion from degrees celsius to Kelvin
        double temperatureKelvin = -56.5 + Constants.DEGREES_CELSIUS_TO_KELVIN ;
        if ( altitudeMeters <= 11000.0) {
        	// troposhere
        	temperatureKelvin = Constants.SEA_LEVEL_TEMPERATURE_KELVIN + 
    			(Constants.TEMPERATURE_LAPSE_RATE_DEG_CELSIUS_PER_FEET * altitudeFeet);
        
		} else {
			if ( ( altitudeMeters > 11000.0 ) && ( altitudeMeters <= 20000.0) ) {
				// normal conversion of degrees celsius to Kelvin
				temperatureKelvin = -56.5 + Constants.DEGREES_CELSIUS_TO_KELVIN ;
	        	System.out.println(" constant temperature -> -56.5 degree celsius = " + temperatureKelvin + " Kelvins");
			} else {
				// raise an exception
				throw new CustomAltitudeMetersRangeException("altitude meters above 20.000 meters");
			}
        } 
        // Calculate temperature at altitude (Kelvin)
        logger.info("Temperature = " + temperatureKelvin + " Kelvin at altitude = " + altitudeFeet + " feet");

        // Calculate speed of sound at altitude (knots)
        double speedOfSoundKnotsAtAltitudeFeet = Constants.SEA_LEVEL_SPEED_OF_SOUND_KNOTS * 
        		Math.sqrt(temperatureKelvin / Constants.SEA_LEVEL_TEMPERATURE_KELVIN);

        // Calculate TAS (True Airspeed)
        double tasKnots = mach * speedOfSoundKnotsAtAltitudeFeet;
        return tasKnots;
	}
	
	/**
	 * 
	 * @return
	 */
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
