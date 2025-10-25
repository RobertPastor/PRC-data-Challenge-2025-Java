package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestReadFuelData_Test {

	@Test
    public void testReadFuelRank() throws IOException {
		
		FuelData fuelData = new FuelData(train_rank.rank );
		fuelData.readParquet();
		
		fuelData.extendFuelWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();
		
		System.out.println( fuelData.getFuelDataTable().shape());
		System.out.println("rank row count = " +  fuelData.getFuelDataTable().rowCount());

		assert fuelData.getFuelDataTable().rowCount() == 24289;
	}
	
	@Test
    public void testReadFuelTrain() throws IOException {
		
		FuelData fuelData = new FuelData(train_rank.train );
		fuelData.readParquet();
		
		fuelData.extendFuelWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();

		System.out.println( fuelData.getFuelDataTable().shape());
		System.out.println("train row count = " +  fuelData.getFuelDataTable().rowCount());
		
		assert fuelData.getFuelDataTable().rowCount() == 131530;

	}
}
