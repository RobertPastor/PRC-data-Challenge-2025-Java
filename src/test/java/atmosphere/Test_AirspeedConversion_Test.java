package atmosphere;

import org.junit.jupiter.api.Test;

public class Test_AirspeedConversion_Test {
	
	@Test
    public void test_AirspeedConvertion_one () throws Exception {
		
		System.out.println("================ mach 2 tas ========================");
		
		AirSpeedConverter airspeedConverter = new AirSpeedConverter();

		double altitudeMeters = 11000.0;
		double mach = 0.85;
		String alt_units = "m";
		String speed_units = "kt";
		double TasKnots = airspeedConverter.mach2tas( mach , altitudeMeters , speed_units , alt_units);
		System.out.println("for mach = " + String.valueOf(mach)  + 
				" - at altitude = " + String.valueOf(altitudeMeters) + " meters" +
				" - tas = " + String.valueOf(TasKnots) + " knots");
	}
	
	@Test
    public void test_AirspeedConvertion_two () throws Exception {
		
		System.out.println("================ mach 2 cas ========================");

		AirSpeedConverter airspeedConverter = new AirSpeedConverter();
		
		double altitudeFeet = 33000.0;
		double mach = 0.85;
		String alt_units = "ft";
		String speed_units = "kt";
		double TasKnots = airspeedConverter.mach2tas( mach , altitudeFeet , speed_units , alt_units);
		System.out.println("for mach = " + String.valueOf(mach)  + 
				" - at altitude = " + String.valueOf(altitudeFeet) + " feet" +
				" - tas = " + String.valueOf(TasKnots) + " knots");
	}
	
	
	@Test
    public void test_AirspeedConvertion_three () throws Exception {
		
		System.out.println("=============== mach 2 cas =========================");

		AirSpeedConverter airspeedConverter = new AirSpeedConverter();
		
		double altitudeFeet = 33000.0;
		double mach = 0.85;
		String alt_units = "ft";
		String speed_units = "kt";
		double CasKnots = airspeedConverter.mach2cas( mach , altitudeFeet , speed_units , alt_units);
		System.out.println("for mach = " + String.valueOf(mach)  + 
				" - at altitude = " + String.valueOf(altitudeFeet) + " feet" +
				" - cas = " + String.valueOf(CasKnots) + " knots");
	}
}
