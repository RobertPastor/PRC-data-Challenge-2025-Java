package fuel;

import java.io.IOException;

import aircrafts.AircraftsData;
import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;

public class CommonToRankAndTrain {
	
	
	public static void CommonToTrainAndRank ( final train_rank train_rank_value , final long maxToBeComputedRow) throws IOException {
		
		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();

		AircraftsData aircraftsData = new AircraftsData();
		aircraftsData.readExcelFile();
		
		FlightListData flightListData = new FlightListData(train_rank_value);
		flightListData.readParquet();
		flightListData.extendWithFlightDateData();

		System.out.println(flightListData.getFlightListDataTable().shape());
		
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
		
		// as flight take-off and landed are now available from flight list 
		// use them to compute relative delta from burnt start and stop
		fuelData.extendRelativeStartEndFromFlightTakeoff();

		// extend with flight data
		fuelData.extendFuelStartEndInstantsWithFlightData( maxToBeComputedRow );
		
		fuelData.generateParquetFileFor();
			
	}

}
