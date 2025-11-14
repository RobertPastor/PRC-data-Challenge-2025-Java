package atmosphere;



public class AirSpeedConverter {
	
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
		 * 
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
		double tasMeterPerSecond = 0.0;
		if ( speed_units == "kt") {
			tasMeterPerSecond = tas * Knots2MeterPerSecond;
		} else {
			if ( speed_units == "m/s" ) {
				// nothing to do
			} else {
				 throw new UnitsNotYetImplemented("BadaSpeed: tas2cas: unknown speed units= " + (speed_units));
			}
		}
		double altitudeMeters = 0.0;
        if ( alt_units == "m" ) {
             altitudeMeters = altitude;
        } else {
            throw new  UnitsNotYetImplemented("BadaSpeed: tas2cas: unknown altitude units= " + (alt_units));
        }
        
        //''' 1.4 adiabatic '''
        double mu = (1.4 - 1.0 ) / 1.4;
        
        double densityKgm3 = atmosphere.getAirDensityKilogramsPerCubicMeters(altitudeMeters);
        double pressurePascals = atmosphere.getPressurePascals(altitudeMeters);
        
        double densityMSLkgm3 = atmosphere.getAirDensitySeaLevelKilogramsPerCubicMeters();
        double pressureMSLpascals = atmosphere.getPressureMeanSeaLevelPascals();
        
        double cas = 1 + ( mu * densityKgm3 * tasMeterPerSecond * tasMeterPerSecond) / ( 2 *  pressurePascals);
        cas = Math.pow(cas , 1.0 / mu) - 1.0;

        cas = 1 + (pressurePascals / pressureMSLpascals) * (cas);
        cas = Math.pow(cas, mu) - 1.0;
        cas = ( 2 * pressureMSLpascals)/ (mu * densityMSLkgm3) * cas;
        cas = Math.pow(cas , 0.5);
        return cas;
        		
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
    
    
    public double cas2tas(final double cas, final double altitude, final String speed_units , final String altitude_units ) {
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
            throw new UnitsNotYetImplemented("Speed: tas2cas: unknown altitude units = " + (altitude_units));
        }
           // ''' 1.4 adiabatic '''
        double mu = (1.4 - 1.0 ) / 1.4;
        
        double densityKgm3 = atmosphere.getAirDensityKilogramsPerCubicMeters(altitudeMeters);
        double pressurePascals = atmosphere.getPressurePascals(altitudeMeters);
        
        double densityMSLkgm3 = atmosphere.getAirDensitySeaLevelKilogramsPerCubicMeters();
        double pressureMSLpascals = atmosphere.getPressureMeanSeaLevelPascals();

        double tas = 1 + ( mu * densityMSLkgm3 * casMetersPerSecond * casMetersPerSecond) / ( 2 *  pressureMSLpascals);
        tas = Math.pow(tas , 1.0 / mu) - 1.0;

        tas = 1 + (pressureMSLpascals / pressurePascals) * (tas);
        tas = Math.pow(tas, mu) - 1.0;
        tas = ( 2 * pressurePascals)/ (mu * densityKgm3) * tas;
        tas = Math.pow(tas , 0.5);
        return tas;
    }
	
      
}
