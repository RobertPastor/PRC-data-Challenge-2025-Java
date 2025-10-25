package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import aircrafts.AircraftsData;
import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;

public class TestExtendFuelDataWithNearestFlightData_Rank {

	@Test
	public void testReadExtendFuelRank() throws IOException {

		train_rank train_rank_value = train_rank.rank;

		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();

		AircraftsData aircraftsData = new AircraftsData();
		aircraftsData.readExcelFile();

		FlightListData flightListData = new FlightListData(train_rank_value);
		flightListData.readParquet();

		flightListData.extendWithAirportData( airportsData );
		//flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();

		//flightListData.extendWithAircraftsData( aircraftsData );
		//flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();

		FuelData fuelData = new FuelData( train_rank_value );
		fuelData.readParquet();

		System.out.println("fuel data table - row count = " +  fuelData.getFuelDataTable().rowCount());

		fuelData.extendFuelWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();

		// merge fuel with flight list
		fuelData.extendFuelWithFlightListData( flightListData.getFlightListDataTable() ) ;

		// extend with flight data
		int maxToBeComputedRow = 1000000;
		fuelData.extendFuelStartEndInstantsWithFlightData( maxToBeComputedRow );

		System.out.println(fuelData.getFuelDataTable().structure());
		System.out.println(fuelData.getFuelDataTable().print(10));

		fuelData.generateParquetFileFor( );
	}
}
