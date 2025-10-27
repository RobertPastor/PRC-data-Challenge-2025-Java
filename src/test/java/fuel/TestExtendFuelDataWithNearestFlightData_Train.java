package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import aircrafts.AircraftsData;
import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;

public class TestExtendFuelDataWithNearestFlightData_Train {

		@Test
	    public void testReadExtendFuelTrain() throws IOException {
			
			train_rank train_rank_value = train_rank.train;
			
			AirportsData airportsData = new AirportsData();
			airportsData.readParquet();

			AircraftsData aircraftsData = new AircraftsData();
			aircraftsData.readExcelFile();
			
			FlightListData flightListData = new FlightListData(train_rank_value);
			flightListData.readParquet();
			System.out.println(flightListData.getFlightListDataTable().shape());
			flightListData.extendWithFlightDateData();
			
			flightListData.extendWithAirportData( airportsData );
			//flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();
			System.out.println(flightListData.getFlightListDataTable().shape());

			flightListData.extendWithAircraftsData( aircraftsData );
			//flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();
			System.out.println(flightListData.getFlightListDataTable().shape());

			FuelData fuelData = new FuelData( train_rank_value );
			fuelData.readParquet();
			
			System.out.println("fuel data table - row count = " +  fuelData.getFuelDataTable().rowCount());
			
			fuelData.extendFuelWithEndStartDifference();
			fuelData.extendFuelFlowKgSeconds();
			
			// merge fuel with flight list
			fuelData.extendFuelWithFlightListData( flightListData.getFlightListDataTable() ) ;

			// as flight take-off and landed are now available use them to compute relative delta from burnt start and stop
			fuelData.extendRelativeStartEndFromFlightTakeoff();

			// extend with flight data
			//int maxToBeComputedRow = 1000000;
			int maxToBeComputedRow = 100;
			fuelData.extendFuelStartEndInstantsWithFlightData( maxToBeComputedRow );
			
			//System.out.println(fuelData.getFuelDataTable().structure());
			//System.out.println(fuelData.getFuelDataTable().print(10));
			
			fuelData.generateParquetFileFor();
			System.out.println(fuelData.getFuelDataTable().print(100));

		}
}
