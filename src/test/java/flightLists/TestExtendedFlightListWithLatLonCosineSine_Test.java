package flightLists;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestExtendedFlightListWithLatLonCosineSine_Test {
	/**
	 * latitude and longitude in degrees are not a good signal for the model
	 * because there should not be a discontinuity from 360 degrees to 0 degrees
	 * 
	 * @throws IOException
	 * Latitude: Values range from -90° to +90°.
	 * The equator is at 0° latitude.
	 * 
	 * Longitude:
	 * Values range from -180° to +180°.
	 * Lines of longitude run north-south and converge at the poles.
	 * The prime meridian (0° longitude) runs through Greenwich, England.
	 * These ranges define the geographic coordinate system used to specify locations on Earth. 
	 */

	@Test
	public  void testExtendFlightList () throws IOException { 

		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();

		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
		
		flightListData.extendWithAirportData(airportsData);
		
		flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();
	}

}
