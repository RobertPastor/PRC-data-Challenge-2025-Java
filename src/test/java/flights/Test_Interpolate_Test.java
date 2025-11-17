package flights;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import tech.tablesaw.api.Table;
import utils.CustomException;

public class Test_Interpolate_Test {
	
	
	@Test
    public void Interpolate_Two() throws IOException, CustomException {
				
		train_rank_final train_rank_final_value = train_rank_final.rank;
		
		System.out.println("================ test two searching for missing lat lon altitudes ==========================");
		
		FlightListData flightListData = new FlightListData(train_rank_final_value );
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
		
		SortedSet<String> listOfUniqueFlightIds = flightListData.getListOfUniqueFlightIds();
		
		int numberOfFiles = listOfUniqueFlightIds.size();
		System.out.println("---> number of flight files = " + String.valueOf(numberOfFiles));
		
		List<String> columnNamesList = Arrays.asList("latitude","longitude","altitude");

		int index = 1;
		for ( String flight_id : listOfUniqueFlightIds) {
			System.out.println(flight_id);
			index++;
			
			FlightData flightData = new FlightData(train_rank_final_value , flight_id);
			flightData.readParquetWithStream();
			
			Table flightDataTable = flightData.getFlightDataTable();
			//for ( String columnName : flightDataTable.columnNames() ) {
				//System.out.println(columnName);
			//}
			
			for ( String columnName : columnNamesList) {
				assert ( flightDataTable.columnNames().contains(columnName) );
				
			}
		}
		System.out.printf("%d", index);

	}
}
