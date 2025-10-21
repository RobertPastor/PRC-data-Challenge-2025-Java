package flightListData;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestComputeHaversineDistance {

	
	@Test
    public void testComputeDistanceNauticalMiles() throws IOException {
		
		double lat1 = 40.714268; // New York
		double lon1 = -74.005974;
		double lat2 = 34.0522; // Los Angeles
		double lon2 = -118.2437;

		FlightListData flightListData = new FlightListData(train_rank.rank );
		double distanceNm = flightListData.calculateDistanceNauticalMiles(lat1, lon1, lat2, lon2);
		System.out.println( distanceNm );
		
}
