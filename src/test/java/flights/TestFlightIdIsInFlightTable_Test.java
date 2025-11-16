package flights;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import utils.CustomException;

public class TestFlightIdIsInFlightTable_Test {

	@Test
    public void testIsFlightIdInFilightTable() throws IOException, CustomException {
		
		String flight_id = "prc806615763";
		
		FlightData flightData = new FlightData(train_rank_final.rank , flight_id);
		flightData.readParquetWithStream();
		
		System.out.println( flightData.getFlightDataTable().print(10));
		
		boolean bool = flightData.flightIdIsExisting(flight_id);
		System.out.println("check if flight id = <<" + flight_id + ">> is in flight table = " +  bool);
	}
}
