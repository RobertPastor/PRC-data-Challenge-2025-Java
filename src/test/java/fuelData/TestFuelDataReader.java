package fuelData;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestFuelDataReader {

	@Test
    public void testReadRankFuelFile() throws IOException {

		FuelData fuelData = new FuelData(train_rank.rank );
		fuelData.readParquet();
		fuelData.extendWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();
	}
	
	@Test
    public void testReadTrainFuelFile() throws IOException {

		FuelData fuelData = new FuelData(train_rank.train );
		fuelData.readParquet();
		fuelData.extendWithEndStartDifference();
		fuelData.extendFuelFlowKgSeconds();
	}
}
