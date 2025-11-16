package flights;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import utils.CustomException;

public class Test_UniqueAircraftIcaoCode_InFlightData_Tests {

	@Test
	public  void test_ListUniqueAircraftIACOcodesTrainFlight  () throws IOException , CustomException { 
		
		System.out.println("==================== test one unique aircrafts in TRAIN  ====================");
		
		train_rank_final train_rank_final_value = train_rank_final.train;
		
		FlightData flightData = new FlightData(train_rank_final_value , "prc806615763");
		flightData.readParquetWithStream();
		
		FlightListData trainflightListData = new FlightListData( train_rank_final_value );
		trainflightListData.readParquet();
		
		
	}
}
