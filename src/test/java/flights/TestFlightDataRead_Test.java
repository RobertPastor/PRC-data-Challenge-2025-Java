package flights;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;

public class TestFlightDataRead_Test {
	
	@Test
    public void testReadFlight() throws IOException {

		FlightData flightData = new FlightData(train_rank_final.rank , "prc806615763");
		flightData.readParquetWithStream();
		
		System.out.println("Row Count -> " + flightData.getFlightDataTable().rowCount() );
		System.out.println("Shape -> " + flightData.getFlightDataTable().shape() );
		
		flightData.extendWithLatitudeLongitudeCosineSine();
		
		System.out.println("Shape -> " + flightData.getFlightDataTable().shape() );
		System.out.println( flightData.getFlightDataTable().print(10) );

	}

}
