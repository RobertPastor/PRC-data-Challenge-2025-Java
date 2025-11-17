package flightLists;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flights.FlightData;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;
import utils.CustomException;

public class Test_SearchForMissingFeaturesFinal_Test {

	
	@Test
    public void searchForMissingFeatures_Two() throws IOException, CustomException {
				
		train_rank_final train_rank_final_value = train_rank_final.final_submission;

		SortedSet<String> flightIdsWithMissingData = new TreeSet<String>();
		
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
			
			System.out.println(index + " / " + String.valueOf( listOfUniqueFlightIds.size() ) + " --> " + flight_id );
			
			FlightData flightData = new FlightData(train_rank_final_value , flight_id);
			flightData.readParquetWithStream();
			
			Table flightDataTable = flightData.getFlightDataTable();
			//for ( String columnName : flightDataTable.columnNames() ) {
				//System.out.println(columnName);
			//}
			
			for ( String columnName : columnNamesList) {
				assert ( flightDataTable.columnNames().contains(columnName) );
				
				Selection selectionIsMissing = flightDataTable.doubleColumn(columnName).isMissing();
				Table filteredTable = flightDataTable.where(selectionIsMissing);
				//System.out.println("for col = " + columnName + " - Missing = " + String.valueOf(filteredTable.rowCount()) + " / "  + String.valueOf(flightDataTable.rowCount()));
				if ( filteredTable.rowCount() > 0 ) {
					flightIdsWithMissingData.add(flight_id);
				}
			}
			
			if ( index > 10000 ) {
				//break;
			}
			index++;
		}
		System.out.println("---> total index = " + String.valueOf(index));
		
		for (String flight_id : flightIdsWithMissingData) {
			System.out.println(flight_id);
		}
	}
}


