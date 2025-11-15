package atmosphere;

import java.util.logging.Logger;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;




public final class Atmosphere {
	
	protected class OutOfBoundException extends RuntimeException {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2456453090484975274L;

		/**
		 * exception raised when a value is not in the expected range
		 */
		protected OutOfBoundException(String message) {
			super(message);
		}
	}

	private static final Logger logger = Logger.getLogger(Atmosphere.class.getName());

	/*
    The standard sea level conditions are as follows:
        Temperature (T0) = 288.15 K = 150C
        Pressure (p0) = 101325 N/m2 = 760 mm of Hg
	 */
	static double SeaLevelTemperatureDegrees = 15.0;
	static double SeaLevelPressureNewtonsSquareMeters = 101325.0;
	// MSL Mean Sea Level 
	static double StandardAtmosphericTemperatureMslKelvins = 288.15; // # kelvins
	static double StandardAtmosphericPressureMslPascal = 101325; //# pascals
	static double StandardAtmosphericDensityMslKgCubicMeters = 1.225; // # [kg/m3]
	static double SpeedOfSoundMslMetersSeconds = 340.294; // # at mean sea level [m/s]

	//ISA temperature gradient with altitude below the tropopause :
	static double betaT = - 0.0065; //[°K/m]

	//betaT = - 0.0065 # [°K/m]

	/**
    Tropopause
    Tropopause is the separation between two different layers: the troposphere, which stands
    below it, and the stratosphere, which is placed above. Its altitude HP,trop is constant when
    expressed in terms of geopotential pressure altitude:
    H p,trop = 11000 [m]
	 */
	static double TropopauseGeoPotentialPressureAltitude = 11000.0; //# meters

	// altitude in Meters -> up to 86 Kilometers
	static double[] AltitudeMeters = { -2000, 0,  2000, 4000, 6000, 8000, 10000,
			12000, 14000, 16000, 18000, 20000, 
			22000, 24000, 26000, 28000, 30000,
			32000, 34000, 36000, 38000, 40000,
			42000, 44000, 46000, 48000, 50000, 
			52000, 54000, 56000, 58000, 60000,
			62000, 64000, 66000, 68000, 70000,
			72000, 74000, 76000, 78000, 80000,
			82000, 84000, 86000 };

	/*
    alt-km    sigma    delta    theta    temp-Kelvin    
    pressure-N-sq-m    dens-kg-cu-m    a-sound-m-s    viscosity-kg-m-s    k-visc-sq-m-s

    n this table from -2 to 86 km in 2 km intervals

    alt is altitude in meters.
    sigma is density divided by sea-level density.
    delta is pressure divided by sea-level pressure.
    theta is temperature divided by sea-level temperature.
    temp is temperature in kelvins.
    press is pressure in newtons per square meter.
    dens is density in kilograms per cubic meter.
    a is the speed of sound in meters per second.
    visc is viscosity in 10**(-6) kilograms per meter-second.
    k.visc is kinematic viscosity in square meters per second.

	 */

	static double[][] TabularAtmosphere = {
			//#  sigma      delta      theta    temp    press  density a  visc  k.visc 
			{ 1.21E+00,1.26E+00,1.0451,301.2,1.28E+05,1.48E+00,347.9,18.51,1.25E-05 },
			{ 1.0     ,1.0     ,1.0   ,288.1,1.01E+05,1.23E+00,340.3,17.89,1.46E-05 },
			{ 8.22E-01,7.85E-01,0.9549,275.2,7.95E+04,1.01E+00,332.5,17.26,1.71E-05 },
			{ 6.69E-01,6.09E-01,0.9098,262.2,6.17E+04,8.19E-01,324.6,16.61,2.03E-05 },
			{ 5.39E-01,4.66E-01,0.8648,249.2,4.72E+04,6.60E-01,316.5,15.95,2.42E-05 },
			{ 4.29E-01,3.52E-01,0.8198,236.2,3.57E+04,5.26E-01,308.1,15.27,2.90E-05 },
			{ 3.38E-01,2.62E-01,0.7748,223.3,2.65E+04,4.14E-01,299.5,14.58,3.53E-05 },
			{ 2.55E-01,1.91E-01,0.7519,216.6,1.94E+04,3.12E-01,295.1,14.22,4.56E-05 },
			{ 1.86E-01,1.40E-01,0.7519,216.6,1.42E+04,2.28E-01,295.1,14.22,6.24E-05 },
			{ 1.36E-01,1.02E-01,0.7519,216.6,1.04E+04,1.67E-01,295.1,14.22,8.54E-05 },
			{ 9.93E-02,7.47E-02,0.7519,216.6,7.57E+03,1.22E-01,295.1,14.22,1.17E-04 },
			{ 7.26E-02,5.46E-02,0.7519,216.6,5.53E+03,8.89E-02,295.1,14.22,1.60E-04 },
			{ 5.27E-02,3.99E-02,0.7585,218.6,4.05E+03,6.45E-02,296.4,14.32,2.22E-04 },
			{ 3.83E-02,2.93E-02,0.7654,220.6,2.97E+03,4.69E-02,297.7,14.43,3.07E-04 },
			{ 2.80E-02,2.16E-02,0.7723,222.5,2.19E+03,3.43E-02,299.1,14.54,4.24E-04 },
			{ 2.05E-02,1.60E-02,0.7792,224.5,1.62E+03,2.51E-02,300.4,14.65,5.84E-04 },
			{ 1.50E-02,1.18E-02,0.7861,226.5,1.20E+03,1.84E-02,301.7,14.75,8.01E-04 },
			{ 1.11E-02,8.77E-03,0.793 ,228.5,8.89E+02,1.36E-02,303.0,14.86,1.10E-03 },
			{ 8.07E-03,6.55E-03,0.8112,233.7,6.63E+02,9.89E-03,306.5,15.14,1.53E-03 },
			{ 5.92E-03,4.92E-03,0.8304,239.3,4.99E+02,7.26E-03,310.1,15.43,2.13E-03 },
			{ 4.38E-03,3.72E-03,0.8496,244.8,3.77E+02,5.37E-03,313.7,15.72,2.93E-03 },
			{ 3.26E-03,2.83E-03,0.8688,250.4,2.87E+02,4.00E-03,317.2,16.01,4.01E-03 },
			{ 2.44E-03,2.17E-03,0.888 ,255.9,2.20E+02,3.00E-03,320.7,16.29,5.44E-03 },
			{ 1.84E-03,1.67E-03,0.9072,261.4,1.70E+02,2.26E-03,324.1,16.57,7.34E-03 },
			{ 1.40E-03,1.30E-03,0.9263,266.9,1.31E+02,1.71E-03,327.5,16.85,9.83E-03 },
			{ 1.07E-03,1.01E-03,0.9393,270.6,1.02E+02,1.32E-03,329.8,17.04,1.29E-02 },
			{ 8.38E-04,7.87E-04,0.9393,270.6,7.98E+01,1.03E-03,329.8,17.04,1.66E-02 },
			{ 6.58E-04,6.14E-04,0.9336,269.0,6.22E+01,8.06E-04,328.8,16.96,2.10E-02 },
			{ 5.22E-04,4.77E-04,0.9145,263.5,4.83E+01,6.39E-04,325.4,16.68,2.61E-02 },
			{ 4.12E-04,3.69E-04,0.8954,258.0,3.74E+01,5.04E-04,322.0,16.40,3.25E-02 },
			{ 3.23E-04,2.83E-04,0.8763,252.5,2.87E+01,3.96E-04,318.6,16.12,4.07E-02 },
			{ 2.53E-04,2.17E-04,0.8573,247.0,2.20E+01,3.10E-04,315.1,15.84,5.11E-02 },
			{ 1.96E-04,1.65E-04,0.8382,241.5,1.67E+01,2.41E-04,311.5,15.55,6.46E-02 },
			{ 1.52E-04,1.24E-04,0.8191,236.0,1.26E+01,1.86E-04,308.0,15.26,8.20E-02 },
			{ 1.17E-04,9.34E-05,0.8001,230.5,9.46E+00,1.43E-04,304.4,14.97,1.05E-01 },
			{ 8.91E-05,6.96E-05,0.7811,225.1,7.05E+00,1.09E-04,300.7,14.67,1.34E-01 },
			{ 6.76E-05,5.15E-05,0.7620,219.6,5.22E+00,8.28E-05,297.1,14.38,1.74E-01 },
			{ 5.09E-05,3.79E-05,0.7436,214.3,3.84E+00,6.24E-05,293.4,14.08,2.26E-01 },
			{ 3.79E-05,2.76E-05,0.7300,210.3,2.80E+00,4.64E-05,290.7,13.87,2.99E-01 },
			{ 2.80E-05,2.01E-05,0.7164,206.4,2.03E+00,3.43E-05,288.0,13.65,3.98E-01 },
			{ 2.06E-05,1.45E-05,0.7029,202.5,1.47E+00,2.52E-05,285.3,13.43,5.32E-01 },
			{ 1.51E-05,1.04E-05,0.6893,198.6,1.05E+00,1.85E-05,282.5,13.21,7.16E-01 },
			{ 1.10E-05,7.40E-06,0.6758,194.7,7.50E-01,1.34E-05,279.7,12.98,9.68E-01 },
			{ 7.91E-06,5.24E-06,0.6623,190.8,5.31E-01,9.69E-06,276.9,12.76,1.32E+00 },
			{ 5.68E-06,3.68E-06,0.6488,186.9,3.73E-01,6.96E-06,274.1,12.53,1.80E+00 } };

	static double[] AtmosphereTemperatureKelvins = new double[TabularAtmosphere.length];
	static double[] AirDensityKilogramsCubicMeters = new double[TabularAtmosphere.length];
	static double[] SpeedOfSoundMetersPerSecond = new double[TabularAtmosphere.length];
	static double[] PressurePascals = new double[TabularAtmosphere.length];

	public Atmosphere() {
		
		//logger.info(String.valueOf( AltitudeMeters.length) );
		//logger.info("table length = " + String.valueOf( TabularAtmosphere.length ) );
		assert AltitudeMeters.length == AltitudeMeters.length;
		
		for ( int rowIndex = 0 ; rowIndex < TabularAtmosphere.length ; rowIndex ++) {
			//logger.info("row index = " + String.valueOf( rowIndex ) );

			for (int colIndex = 0; colIndex < TabularAtmosphere[rowIndex].length; colIndex++) {
				//logger.info("row index = " + String.valueOf( rowIndex ) + " - column index = " +String.valueOf( colIndex ) );

				//System.out.println(TabularAtmosphere[rowIndex][colIndex]);
				if (colIndex == 1) {
					PressurePascals[rowIndex] = TabularAtmosphere[rowIndex][colIndex];
				}
				if (colIndex == 3) {
					AtmosphereTemperatureKelvins[rowIndex] = TabularAtmosphere[rowIndex][colIndex];
				}
				if (colIndex == 5) {
					AirDensityKilogramsCubicMeters[rowIndex] = TabularAtmosphere[rowIndex][colIndex];
				}
				if (colIndex == 6) {
					SpeedOfSoundMetersPerSecond[rowIndex] = TabularAtmosphere[rowIndex][colIndex];
					//logger.info("speed of sound = "+ String.valueOf(SpeedOfSoundMetersPerSecond[rowIndex]));
				}
			}
		}
	}
	
    public double getPressureMeanSeaLevelPascals() {
    	double altitudeMeters = 0.0;
        return this.getPressurePascals(altitudeMeters);
    }
    
    public double getAirDensitySeaLevelKilogramsPerCubicMeters() {
    	double altitudeMeters = 0.0;
    	return this.getAirDensityKilogramsPerCubicMeters(altitudeMeters);
    }

	public double getAirDensityKilogramsPerCubicMeters( final double altitudeMeters) throws OutOfBoundException {
		if ( (altitudeMeters > -1999.0) && (altitudeMeters <= 86000.0) ) {
			// build a double array as expected by the Apache.math3 interpolation function

			LinearInterpolator interpolator = new LinearInterpolator();
			PolynomialSplineFunction interpolateFunction = interpolator.interpolate(AltitudeMeters, AirDensityKilogramsCubicMeters);

			if (  interpolateFunction.isValidPoint(altitudeMeters)) {
				double airDensityKgPerCubicMeters = interpolateFunction.value(altitudeMeters);
				//logger.info("interpolation for altitude meters = " + String.valueOf(altitudeMeters) + " - air density = " + String.valueOf(airDensityKgPerCubicMeters) + " kg / m3");
				return airDensityKgPerCubicMeters;
			} else {
				// return Not a Number
				return 0.0/0.0;
			}
		} else {
			String message = "Atmosphere - altitude Meters argument out of bound = " + String.valueOf(altitudeMeters);
			throw new OutOfBoundException (message);
		}
	}
	
	public double getTemperatureKelvins(final double altitudeMeters) throws OutOfBoundException {
        if ( (altitudeMeters > -1999.0) && (altitudeMeters <= 86000.0) ) {
        	
			LinearInterpolator interpolator = new LinearInterpolator();
			PolynomialSplineFunction interpolateFunction = interpolator.interpolate(AltitudeMeters, AtmosphereTemperatureKelvins);

			if (  interpolateFunction.isValidPoint(altitudeMeters)) {
				double temperatureKelvins = interpolateFunction.value(altitudeMeters);
	            // The temperature T in degrees Celsius (�C) is equal to the temperature T in Kelvin (K) minus 273.15:   
	            // temperatureDegrees = temperatureKelvins - 273.15
				//logger.info("interpolation for altitude meters = " + String.valueOf(altitudeMeters) + " - temperature = " + String.valueOf(temperatureKelvins) + " Kelvins");
				return temperatureKelvins;
			} else {
				// return Not a Number
				return 0.0/0.0;
			}
        } else {
			String message = "Atmosphere - altitude Meters argument out of bound = " + String.valueOf(altitudeMeters);
			throw new Atmosphere.OutOfBoundException (message);
		}
	}
	
	public double getTemperatureDegreeCelsius(final double altitudeMeters ) throws OutOfBoundException {
		double temperatureKelvins = getTemperatureKelvins(altitudeMeters);
        // The temperature T in degrees Celsius (�C) is equal to the temperature T in Kelvin (K) minus 273.15:   
		return temperatureKelvins - 273.15;
	}
	
	 public double getSpeedOfSoundMetersPerSecond( final double altitudeMeters) throws OutOfBoundException {
		 
		 if ( (altitudeMeters > -1999.0) && (altitudeMeters <= 86000.0) ) {
	        	
				LinearInterpolator interpolator = new LinearInterpolator();
				PolynomialSplineFunction interpolateFunction = interpolator.interpolate(AltitudeMeters, SpeedOfSoundMetersPerSecond);

				if (  interpolateFunction.isValidPoint(altitudeMeters)) {
					double speedOfSound = interpolateFunction.value(altitudeMeters);
		            
					//logger.info("interpolation for altitude meters = " + String.valueOf(altitudeMeters) + " - speed of sound = " + String.valueOf(speedOfSound) );
					return speedOfSound;
				} else {
					// return Not a Number
					return 0.0/0.0;
				}
	        } else {
				String message = "Atmosphere - altitude Meters argument out of bound = " + String.valueOf(altitudeMeters);
				throw new Atmosphere.OutOfBoundException (message);
			}
	 }
	 
	 public double getPressurePascals( final double altitudeMeters) throws OutOfBoundException {
		 
		 if ( (altitudeMeters > -1999.0) && (altitudeMeters <= 86000.0) ) {
	        	
				LinearInterpolator interpolator = new LinearInterpolator();
				PolynomialSplineFunction interpolateFunction = interpolator.interpolate(AltitudeMeters, PressurePascals);

				if (  interpolateFunction.isValidPoint(altitudeMeters)) {
					double pressurePascals = interpolateFunction.value(altitudeMeters);
		            
					//logger.info("interpolation for altitude meters = " + String.valueOf(altitudeMeters) + " - pressure = " + String.valueOf(pressurePascals) + " Pascals");
					return pressurePascals;
				} else {
					// return Not a Number
					return 0.0/0.0;
				}
	        } else {
				String message = "Atmosphere - altitude Meters argument out of bound = " + String.valueOf(altitudeMeters);
				throw new Atmosphere.OutOfBoundException (message);
			}
	 }
}
