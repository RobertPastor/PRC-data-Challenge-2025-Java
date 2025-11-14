package atmosphere;


import org.junit.jupiter.api.Test;

public class Test_Atmosphere_Tests {
		
		@Test
	    public void test_Atmosphere_one () throws Exception {
			
			Atmosphere atmosphere = new Atmosphere();
			double altitudeMeters = 11000.0;
			double temperatureKelvins = atmosphere.getTemperatureKelvins(altitudeMeters);
			System.out.println( "temperature = " + String.valueOf( temperatureKelvins ) + " Kelvins");
			
			double temperatureDegreesCelsius = atmosphere.getTemperatureDegreeCelsius(altitudeMeters);
			System.out.println( "temperature = " + String.valueOf( temperatureDegreesCelsius ) + " Degrees Celsius");
		}
		
		@Test
		public void test_Atmosphere_two() {
			
			double altitudeMeters = 9000.0;
			Atmosphere atmosphere = new Atmosphere();
			double airDensity = atmosphere.getAirDensityKilogramsPerCubicMeters(altitudeMeters);
			System.out.println( String.valueOf( airDensity ) );
		}
		
		@Test
		public void test_Atmosphere_three() {
			
			double altitudeMeters = 8000.0;
			Atmosphere atmosphere = new Atmosphere();
			double pressurePascals = atmosphere.getPressurePascals(altitudeMeters);
			System.out.println( String.valueOf( pressurePascals ) );
		}
}
