package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import aircrafts.AircraftsData;
import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;

public class TestExtendFuelDataWithFlightListData_Test {

	@Test
    public void testReadRankFuelFile() throws IOException {
		
		int nbMaxRows = 100;
		
		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();

		AircraftsData aircraftsData = new AircraftsData();
		aircraftsData.readExcelFile();
		
		train_rank_final train_rank_value = train_rank_final.rank;
		FlightListData flightListData = new FlightListData(train_rank_value);
		flightListData.readParquet();
		
		flightListData.extendWithAircraftsData( aircraftsData );
		flightListData.extendWithAirportData( airportsData );
		flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();

		FuelData fuelData = new FuelData(train_rank_value , nbMaxRows );
		fuelData.readParquet();
		
		fuelData.extendFuelWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();
		
		// merge fuel with flight list
		fuelData.extendFuelWithFlightListData( flightListData.getFlightListDataTable() ) ;
		
		System.out.println(fuelData.getFuelDataTable().structure());
		System.out.println(fuelData.getFuelDataTable().print(10));
		System.out.println("train-rank -> " + train_rank_value + " ---> " +fuelData.getFuelDataTable().shape());
		
		fuelData.generateParquetFileFor();

	}
	
	@Test
    public void testReadTrainFuelFile() throws IOException {
		
		int nbMaxRows = 100;
		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();
		
		AircraftsData aircraftsData = new AircraftsData();
		aircraftsData.readExcelFile();

		train_rank_final train_rank_value = train_rank_final.train;
		
		FlightListData flightListData = new FlightListData(train_rank_value);
		flightListData.readParquet();
		
		flightListData.extendWithAircraftsData( aircraftsData );
		flightListData.extendWithAirportData( airportsData );
		flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();

		FuelData fuelData = new FuelData(train_rank_value , nbMaxRows );
		fuelData.readParquet();
		
		fuelData.extendFuelWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();
		
		fuelData.extendFuelWithFlightListData( flightListData.getFlightListDataTable() );
		System.out.println(fuelData.getFuelDataTable().structure());
		System.out.println(fuelData.getFuelDataTable().print(10));
		System.out.println("train-rank -> " + train_rank_value + " ---> " +fuelData.getFuelDataTable().shape());

	}
}
