package flightLists;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank_final;

public class TestExtendFlightListWithAirportData_Test {
	
	@Test
    public void testExtendFlightList() throws IOException {
		
		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();

		FlightListData flightListData = new FlightListData(train_rank_final.rank );
		flightListData.readParquet();
		
		flightListData.extendWithAirportData(airportsData);
		
		flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();
	}

}
