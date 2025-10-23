package utils;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import utils.Utils;

public class TestComputeHaversineDistance_Test {

	@Test
    public void testComputeDistanceNauticalMiles() throws IOException {
		
		double lat1 = 40.714268; // New York
		double lon1 = -74.005974;
		double lat2 = 34.0522; // Los Angeles
		double lon2 = -118.2437;

		double distanceNm = Utils.calculateHaversineDistanceNauticalMiles(lat1, lon1, lat2, lon2);
		System.out.println("distance (nautical miles) = " +  distanceNm );
		System.out.println("distance (kilometers) = " +  (distanceNm * Constants.nautical_miles_to_kilometers));
		assert (distanceNm > 2445.0 ) && (distanceNm < 2446.0);
		
	}
}
