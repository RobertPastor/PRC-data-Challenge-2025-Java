package flightData;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;


public class TestFlightDataRead {
	
	@Test
    public void testReadFlight() throws IOException {

		FlightData flightData = new FlightData(train_rank.rank , "prc806615763");
		flightData.read();
	}

}
