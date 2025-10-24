package flightLists;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import aircrafts.AircraftsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestExtendWithAircraftsData_Test {

	@Test
    public void testExtendFlightList() throws IOException {
		
		AircraftsData aircraftsData = new AircraftsData();
		aircraftsData.readExcelFile();
		System.out.println(aircraftsData.getAircraftDataTable().shape());
		System.out.println(aircraftsData.getAircraftDataTable().print(10));

		// rank
		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
		
		flightListData.extendWithAircraftsData(aircraftsData);
		
	}
	
}
