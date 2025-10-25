package utils;

import java.io.IOException;


import org.junit.jupiter.api.Test;
import utils.Utils;

public class TestLatitudeConverter_Test {

	
	@Test
	public  void testLatitudeConverter_one () throws IOException { 
	
        double latitude = 45.0; // Example latitude
        double convertedLatitude = Utils.convertLatitudeTo360(latitude);
        System.out.println("Converted Latitude: " + convertedLatitude);
    }
	
	@Test
	public  void testLatitudeConverter_two () throws IOException { 
		
		double latitude = -90.0;
		for ( int index = 0 ; index < 1000 ; index ++ ) {
			
			latitude = latitude + index;
			if ( latitude < 90.0 ) {
				
				double convertedLatitude = Utils.convertLatitudeTo360(latitude);
				System.out.println("initial latitude = " + latitude + " --> Converted Latitude: " + convertedLatitude);
			}
		}
    }
	
}
