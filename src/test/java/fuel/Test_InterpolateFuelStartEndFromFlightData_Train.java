package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;

public class Test_InterpolateFuelStartEndFromFlightData_Train {

		@Test
	    public void testReadExtendFuelTrain() throws IOException {
			
			train_rank train_rank_value = train_rank.train;
			
			long maxToBeComputedRow = 1000000;
			maxToBeComputedRow = 100;
			
			FuelData fuelData = new FuelData( train_rank_value );
			fuelData.readParquet();
			System.out.println( fuelData.getFuelDataTable().shape());
			System.out.println( fuelData.getFuelDataTable().structure().print());
			
			fuelData.CommonToTrainAndRank( maxToBeComputedRow );
			
		}
}
