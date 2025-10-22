package airports;

import java.io.IOException;

import org.junit.jupiter.api.Test;


public class TestAirportsDataReader {

	@Test
    public void testReadAirports () throws IOException {

		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();
	}

}
