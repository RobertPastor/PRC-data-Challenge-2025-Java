package flightLists;

import java.io.IOException;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

import aircrafts.AircraftsData;
import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import fuelWithTableSplitter.Test_FuelTableFillwithExecutors_Test;

public class TestExtendWithAircraftsData_Test {

	private static final Logger logger = Logger.getLogger(TestExtendWithAircraftsData_Test.class.getName());

	@Test
    public void testExtendFlightList() throws IOException {
		
		AircraftsData aircraftsData = new AircraftsData();
		aircraftsData.readExcelFile();
		
		System.out.println(aircraftsData.getAircraftDataTable().shape());
		System.out.println(aircraftsData.getAircraftDataTable().print(10));

		// rank
		FlightListData flightListData = new FlightListData(train_rank_final.rank );
		flightListData.readParquet();
		
		flightListData.extendWithAircraftsData(aircraftsData);
		System.out.println(aircraftsData.getAircraftDataTable().shape());
		logger.info(aircraftsData.getAircraftDataTable().print(10));

	}
	
}
