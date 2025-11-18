package flightLists;

import java.io.IOException;
import java.util.Set;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import tech.tablesaw.api.Table;

public class Test_CreateInterpolatedFlightsFile_Test {

	
	@Test
    public void loopThroughtListOfFlightIds() throws IOException {
		
		train_rank_final train_rank_final_value = train_rank_final.train;
		FlightListData flightListData = new FlightListData( train_rank_final_value);
		flightListData.readParquet();
		
		Table flightListTable = flightListData.getFlightListDataTable();
		System.out.println("shape = " + flightListTable.shape() );
		
		Set<String> setOfFlightIds = flightListData.getFlightListDataTable().stringColumn("flight_id").asSet();
		int index = 1;
		for ( String flight_id : setOfFlightIds) {
			System.out.println( index + " --- " + flight_id );
			index++;
		}
	}
	
}
