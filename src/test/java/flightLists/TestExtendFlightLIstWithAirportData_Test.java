package flightLists;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;

public class TestExtendFlightLIstWithAirportData_Test {
	
	@Test
    public void testExtendFlightList() throws IOException {
		
		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();

		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
		
		flightListData.extendWithAirportData(airportsData);
		
		flightListData.extendWithSinusCosinusOfLatitudeLongitude();
	}

}
