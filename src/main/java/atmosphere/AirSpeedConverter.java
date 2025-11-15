package atmosphere;

import java.util.logging.Logger;

import utils.Constants;

public class AirSpeedConverter {

	private static final Logger logger = Logger.getLogger(AirSpeedConverter.class.getName());

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

	protected class UnitsNotYetImplemented extends RuntimeException {

		/**
		 * serial ID
		 */
		private static final long serialVersionUID = 649382913368899005L;

		/**
		 * exception raised when a value is not in the expected range
		 */
		protected UnitsNotYetImplemented(String message) {
			super(message);
		}
	}

	double MeterPerSecond2Knots = 1.94384449 ;
	double Knots2MeterPerSecond = 0.514444444 ;

	private Atmosphere atmosphere ;

	public AirSpeedConverter() {
		atmosphere = new Atmosphere();
	}

	public double tas2cas( final double tas , final double altitude , 
			final String temperature , final String speed_units , final String alt_units )  {

		// temperature = 'std' 
		// speed units =  'm/s'
		// alt_units = 'm'
		if ( temperature == "std") {
			// do nothing and continue
		} else {
			throw new UnitsNotYetImplemented("Speed: tas2cas: unknown speed units= " + (speed_units));
		}
		// computations are done in SI units -> need to convert kt to meters per seconds
		double tasMeterPerSecond = 0.0;
		if ( speed_units == "kt") {
			tasMeterPerSecond = tas * Knots2MeterPerSecond;
		} else {
			if ( speed_units == "m/s" ) {
				// nothing to do
			} else {
				throw new UnitsNotYetImplemented("Speed: tas2cas: unknown speed units= " + (speed_units));
			}
		}
		double altitudeMeters = 0.0;
		if ( alt_units == "m" ) {
			altitudeMeters = altitude;
		} else {
			if ( alt_units == "ft") {
				altitudeMeters = altitude * Constants.feetToMeters;
				//logger.info(String.valueOf(altitudeMeters));
			} else {
				throw new UnitsNotYetImplemented("Speed: tas2cas: unknown altitude units= " + (alt_units));
			}
		}

		//''' 1.4 adiabatic '''
		double mu = (1.4 - 1.0 ) / 1.4;

		double densityKgm3 = atmosphere.getAirDensityKilogramsPerCubicMeters(altitudeMeters);
		double pressurePascals = atmosphere.getPressurePascals(altitudeMeters);

		double densityMSLkgm3 = atmosphere.getAirDensitySeaLevelKilogramsPerCubicMeters();
		double pressureMSLpascals = atmosphere.getPressureMeanSeaLevelPascals();

		// 15th November 2025 -  to be double checked
		pressurePascals = pressurePascals * pressureMSLpascals;
		densityKgm3 = densityKgm3 * densityMSLkgm3;

		double cas = 1 + ( mu * densityKgm3 * tasMeterPerSecond * tasMeterPerSecond) / ( 2 *  pressurePascals);
		//     A = np.power(1 + const.Amu * rho * arr_tas**2 / (2 * p), 1 / const.Amu) - 1
		cas = Math.pow(cas , 1.0 / mu) - 1.0;

		cas = 1 + (pressurePascals / pressureMSLpascals) * (cas);
		// B = np.power(1 + arr_delta * A, const.Amu) - 1
		cas = Math.pow(cas, mu) - 1.0;

		// np.sqrt(2 * const.p_0 * B / (const.Amu * const.rho_0))
		cas = ( 2 * pressureMSLpascals * cas) / (mu * densityMSLkgm3);
		cas = Math.pow(cas , 0.5);
		return cas;

	}

	public double mach2cas( final double mach , final double altitude , 
			final String speed_units , final String alt_units ) {

		//logger.info("speed_units = " + speed_units);
		//logger.info("alt_units = " + alt_units);

		double tas = this.mach2tas(mach , altitude , speed_units, alt_units );
		//logger.info("tas = " + String.valueOf( tas ));

		/*
		 * public double tas2cas( final double tas , final double altitude , 
		 *	final String temperature , final String speed_units , final String alt_units )  {
		 */
		return this.tas2cas( tas, altitude, "std" , speed_units , alt_units) ;

	}

	public double mach2tas (final double mach , final double altitude , 
			final String speed_units , final String alt_units ) throws UnitsNotYetImplemented {

		double tas = 0.0/0.0;
		double altitudeMeters = 0.0;
		// if speed units = kt then retrieved tas must be in knots
		if ( alt_units == "m") {
			altitudeMeters = altitude;
			double speedOfSoundMetersPerSecond = atmosphere.getSpeedOfSoundMetersPerSecond(altitudeMeters);
			double tasMetersPerSeconds = mach * speedOfSoundMetersPerSecond;
			if ( speed_units == "m/s" ) {
				tas = tasMetersPerSeconds;
			} else {
				if ( speed_units == "kt" ) {
					tas = tasMetersPerSeconds * MeterPerSecond2Knots;
				} else {
					throw new UnitsNotYetImplemented("Speed: tas2cas: unknown altitude units= " + (alt_units));
				}
			}
		} else {
			if ( alt_units == "ft") {
				// input altitude is in feet 
				altitudeMeters = altitude * Constants.feetToMeters;
				double speedOfSoundMetersPerSecond = atmosphere.getSpeedOfSoundMetersPerSecond(altitudeMeters);
				double tasMetersPerSeconds = mach * speedOfSoundMetersPerSecond;
				if ( speed_units == "m/s" ) {
					tas = tasMetersPerSeconds;
				} else {
					if ( speed_units == "kt" ) {
						tas = tasMetersPerSeconds * MeterPerSecond2Knots;
					} else {
						throw new  UnitsNotYetImplemented("Speed: tas2cas: unknown altitude units= " + (alt_units));
					}
				}
			} else {
				throw new  UnitsNotYetImplemented("Speed: tas2cas: unknown altitude units= " + (alt_units));
			}
			
		}
		assert Double.isNaN(tas) == false;
		//logger.info("for mach = " + String.valueOf(mach)  + " - at altitude = " + String.valueOf(altitudeMeters) + " meters" +
		//		" - tas = " + String.valueOf(tas) + " - " + speed_units);
		return tas;
	}

	public double tas2mach(final double tas , final double altitude , final String speed_units , final String alt_units) {
		//  mach = TAS / speed of sound  '''
		// speed_units = 'm/s'
		// alt_units = 'm'
		assert ( speed_units == "m/s");
		assert ( alt_units == "m");
		double a = atmosphere.getSpeedOfSoundMetersPerSecond(altitude);
		return tas / a;
	}


	public double cas2tas(final double cas, final double altitude, 
			final String speed_units , final String altitude_units ) {
		// speed_units = 'm/s'
		// altitude_units = 'm'
		double casMetersPerSecond = 0.0;
		if ( speed_units == "kt" ) {
			casMetersPerSecond = cas * Knots2MeterPerSecond;
		} else {
			if ( speed_units == "m/s" ) {
				// nothing to do
			} else {
				throw new UnitsNotYetImplemented("Speed: tas2cas: unknown speed units = " + (speed_units));
			}
		}

		double altitudeMeters = 0.0;
		if ( altitude_units == "m" ) {
			altitudeMeters = altitude;
		} else {
			if (altitude_units == "ft") {
				altitudeMeters = altitude * Constants.feetToMeters;
			} else {
				throw new UnitsNotYetImplemented("Speed: tas2cas: unknown altitude units = " + (altitude_units));
			}
		}
		// ''' 1.4 adiabatic '''
		double mu = (1.4 - 1.0 ) / 1.4;

		double densityKgm3 = atmosphere.getAirDensityKilogramsPerCubicMeters(altitudeMeters);
		double pressurePascals = atmosphere.getPressurePascals(altitudeMeters);

		double densityMSLkgm3 = atmosphere.getAirDensitySeaLevelKilogramsPerCubicMeters();
		double pressureMSLpascals = atmosphere.getPressureMeanSeaLevelPascals();

		// 15th November 2025 -  to be double checked
		pressurePascals = pressurePascals * pressureMSLpascals;
		densityKgm3 = densityKgm3 * densityMSLkgm3;

		double tas = 1 + ( mu * densityMSLkgm3 * casMetersPerSecond * casMetersPerSecond) / ( 2 *  pressureMSLpascals);
		tas = Math.pow(tas , 1.0 / mu) - 1.0;

		tas = 1 + (pressureMSLpascals / pressurePascals) * (tas);
		tas = Math.pow(tas, mu) - 1.0;
		tas = ( 2 * pressurePascals)/ (mu * densityKgm3) * tas;
		tas = Math.pow(tas , 0.5);
		return tas;
	}


}
