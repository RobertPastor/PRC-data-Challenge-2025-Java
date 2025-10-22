package flightListData;

import java.io.IOException;

import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestExtendedFlightListWithLatLonCosineSine {

	public static void main(String[] args) throws IOException {

		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();

		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
		
		flightListData.extendWithAirportData(airportsData);
		
		flightListData.extendWithSinusCosinusOfLatitudeLongitude();
	}

}
