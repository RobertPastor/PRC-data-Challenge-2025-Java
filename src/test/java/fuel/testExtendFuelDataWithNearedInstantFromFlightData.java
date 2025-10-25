package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;

public class testExtendFuelDataWithNearedInstantFromFlightData {

		@Test
	    public void testReadExtendFuelRank() throws IOException {
			
			train_rank train_rank_value = train_rank.train;
			FuelData fuelData = new FuelData( train_rank_value );
			fuelData.readParquet();
			
			System.out.println("fuel data table - row count = " +  fuelData.getFuelDataTable().rowCount());
			
			fuelData.extendFuelStartEndInstantsWithFlightData();
			
			System.out.println(fuelData.getFuelDataTable().structure());
			System.out.println(fuelData.getFuelDataTable().print(10));
			
			fuelData.generateParquetFileFor();

		}
}
