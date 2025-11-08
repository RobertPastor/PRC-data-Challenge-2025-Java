package utils;

public class Constants {

	public static double lbs_to_kilograms = 0.45359237;
	
	public static double kilometers_to_nautical_miles = 0.5399568;
	public static double nautical_miles_to_kilometers = 1.609344;
	
	public static double MetersSecondsToKnots = 1.94384 ;
	
	public static double EARTH_RADIUS_kilometers = 6371.0;
	public static double Kilometers_to_Nautical_Miles = 0.6213711922 ;

	public static double feet_to_meters = 0.3048;
    public static final double feetToMeters = 0.3048;

	public static double meter_to_feets = 3.2808399;
	
	public static double KnotsToMetersSeconds = 0.51444444;
	
	// Constants for ISA (International Standard Atmosphere)
    public static final double SEA_LEVEL_SPEED_OF_SOUND_KNOTS = 661.47; // knots
    
    //https://en.wikipedia.org/wiki/International_standard_atmosphere
    /* As an average, the International Civil Aviation Organization (ICAO) defines an international standard atmosphere (ISA) 
     * with a temperature lapse rate of 6.50 °C/km[7] (3.56 °F or 1.98 °C/1,000 ft) from sea level to 11 km (36,090 ft or 6.8 mi)
     * From 11 km up to 20 km (65,620 ft or 12.4 mi), 
     * the constant temperature is −56.5 °C (−69.7 °F), which is the lowest assumed temperature in the ISA
     */
    public static final double TEMPERATURE_LAPSE_RATE_DEG_CELSIUS_PER_FEET = -0.0019812; // °C/ft
    
    public static final double SEA_LEVEL_TEMPERATURE_KELVIN = 288.15; // Kelvin
    public static final double SEA_LEVEL_PRESSURE_PASCAL = 101325; // Pascals
    
    public static final double GAS_CONSTANT_JOULE_PER_KG_KELVIN = 287.05; // J/(kg·K)
    
    public static final double gasConstantForAirJoulePerMoleKelvins =  8.3144598; // in J/(mol·K)

    public static final double GRAVITY_M_S2= 9.80665; // m/s²
    
    public static final double DEGREES_CELSIUS_TO_KELVIN = 274.15;
    
    // Constants
    public static final double seaLevelPressurePascals = 101325; // Pa
    public static final double seaLevelTemperatureKelvin = 288.15; // K
    public static final double TEMPERATURE_LAPSE_RATE_KELVIN_PER_METER = -0.0065; // K/m
    public static final double gasConstant = 287.05; // J/(kg·K)
    public static final double gammaAdiabaticIndex = 1.4; // Adiabatic index
    
    public static final double MolarMassAirKilogramsPerMole = 0.0289644; // in kg/mol

    public static final double gravitationalAccelerationMetersSecondsSquare = 9.80665; // Gravitational acceleration in m/s^2

}
