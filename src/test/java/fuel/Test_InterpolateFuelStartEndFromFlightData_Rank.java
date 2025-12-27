package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;

public class Test_InterpolateFuelStartEndFromFlightData_Rank {

	@Test
	public void testReadExtendFuelRank() throws IOException {

		train_rank_final train_rank_value = train_rank_final.rank;
		
		FlightListData flightListData = new FlightListData(train_rank_value, "A320");
		flightListData.readParquet();

		long maxToBeComputedRow = 1000000;
		//int maxToBeComputedRow = 100;

		FuelData fuelData = new FuelData(train_rank_value , maxToBeComputedRow);
		fuelData.readParquet(flightListData);
		
		System.out.println( fuelData.getFuelDataTable().shape());
		System.out.println( fuelData.getFuelDataTable().structure().print());

		//fuelData.prepareBeforeMergeFueltoOtherData(maxToBeComputedRow);

	}
}
