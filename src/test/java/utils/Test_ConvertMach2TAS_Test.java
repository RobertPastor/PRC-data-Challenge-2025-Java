package utils;

public class Test_ConvertMach2TAS_Test {


	public static void main(String[] args) throws CustomAltitudeMetersRangeException, CustomExceptionNotYetImplemented {
		double mach = 0.85; // Example Mach number
		double altitudeFeet = 35000; // Example altitude in feet
		
		System.out.println("---------------mach to TAS------------------");

		double tasKnots = Utils.convertMachToTASknots(mach, altitudeFeet);
		System.out.printf("At Mach %.2f and altitude %.0f feet, the TAS is %.3f knots.%n", mach, altitudeFeet, tasKnots);
		
		System.out.println("---------------mach to CAS-----------------");
		double casKnots = Utils.convertMachToCASknots_Not_Working ( mach , altitudeFeet );
		System.out.printf("At Mach %.2f and altitude %.0f feet, the CAS is %.3f knots.%n", mach, altitudeFeet, casKnots);

		System.out.println("---------------mach to TAS------------------");

		altitudeFeet = 50000.0;
		tasKnots = Utils.convertMachToTASknots(mach, altitudeFeet);
		System.out.printf("At Mach %.2f and altitude %.0f feet, the TAS is %.3f knots.%n", mach, altitudeFeet, tasKnots);
		
		System.out.println("----------------mach to CAS-----------------");

		tasKnots = Utils.convertMachToCASknots_Not_Working(mach, altitudeFeet);
		System.out.printf("At Mach %.2f and altitude %.0f feet, the TAS is %.3f knots.%n", mach, altitudeFeet, tasKnots);
	}
}



