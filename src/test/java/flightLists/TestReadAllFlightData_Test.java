package flightLists;

import java.io.IOException;
import java.util.Set;
import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flights.FlightData;

public class TestReadAllFlightData_Test {

	@Test
    public void testReadAllFilghtDataFiles() throws IOException {
		
		FlightListData flightListData = new FlightListData(train_rank_final.rank);
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
		
		Set<String> setOfFlightIds = flightListData.getFlightListDataTable().stringColumn("flight_id").asSet();
		int index = 1;
		for ( String flight_id : setOfFlightIds) {
			System.out.println( index + " --- " + flight_id );
			
			FlightData flightData = new FlightData(train_rank_final.rank , flight_id);
			flightData.readParquetWithStream();
			if ( index > 100 ) {
				break;
			}
			index++;
		}
	}
	
}
