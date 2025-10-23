package flights;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import flights.FlightData;

public class TestFlightDataRead {
	
	@Test
    public void testReadFlight() throws IOException {

		FlightData flightData = new FlightData(train_rank.rank , "prc806615763");
		flightData.readParquet();
		
		System.out.println("Row Count -> " + flightData.getFlightDataTable().rowCount() );
		System.out.println("Shape -> " + flightData.getFlightDataTable().shape() );
		
		flightData.extendWithLatitudeLongitudeCosineSine();
		
		System.out.println("Shape -> " + flightData.getFlightDataTable().shape() );
		System.out.println( flightData.getFlightDataTable().print(10) );

	}

}
