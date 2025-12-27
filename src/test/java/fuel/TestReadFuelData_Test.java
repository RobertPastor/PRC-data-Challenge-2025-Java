package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;

public class TestReadFuelData_Test {

	@Test
    public void testReadFuelRank() throws IOException {
				
		// filter on one aircraft type code (for instance A320)
		FlightListData flightListData = new FlightListData(train_rank_final.rank, "A320");
		flightListData.readParquet();
		
		int nbMaxRows = 100;
		FuelData fuelData = new FuelData(train_rank_final.rank , nbMaxRows );
		fuelData.readParquet(flightListData);
		
		fuelData.extendFuelWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();
		
		System.out.println( fuelData.getFuelDataTable().shape());
		System.out.println("rank row count = " +  fuelData.getFuelDataTable().rowCount());

		assert fuelData.getFuelDataTable().rowCount() == 24289;
	}
	
	@Test
    public void testReadFuelTrain() throws IOException {
		
		// filter on one aircraft type code (for instance A320)
		FlightListData flightListData = new FlightListData(train_rank_final.train, "A320");
		flightListData.readParquet();
		
		int nbMaxRows = 100;
		FuelData fuelData = new FuelData(train_rank_final.train , nbMaxRows);
		fuelData.readParquet(flightListData);
		
		fuelData.extendFuelWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();

		System.out.println( fuelData.getFuelDataTable().shape());
		System.out.println("train row count = " +  fuelData.getFuelDataTable().rowCount());
		
		assert fuelData.getFuelDataTable().rowCount() == 131530;

	}
}
