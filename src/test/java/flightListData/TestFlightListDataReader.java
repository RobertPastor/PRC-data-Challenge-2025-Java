package flightListData;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightListData.FlightListData;

public class TestFlightListDataReader {

	
	@Test
    public void testReadFlightList() throws IOException {

		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
	}
}
