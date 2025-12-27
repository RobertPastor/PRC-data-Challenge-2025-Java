package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;

public class Test_InterpolateFuelStartEndFromFlightData_Train {

		@Test
	    public void testReadExtendFuelTrain() throws IOException {
			
			train_rank_final train_rank_value = train_rank_final.train;
			
			FlightListData flightListData = new FlightListData(train_rank_value);
			flightListData.readParquet();
			
			long maxToBeComputedRow = 1000000;
			//maxToBeComputedRow = 100;
			
			FuelData fuelData = new FuelData( train_rank_value , maxToBeComputedRow);
			fuelData.readParquet(flightListData);
			
			System.out.println( fuelData.getFuelDataTable().shape());
			System.out.println( fuelData.getFuelDataTable().structure().print());
			
			// do all the merge of data external to the final Fuel frame
			//fuelData.prepareBeforeMergeFueltoOtherData( maxToBeComputedRow );
			
		}
}
