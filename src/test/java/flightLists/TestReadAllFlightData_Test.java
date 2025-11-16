package flightLists;

import java.io.IOException;
import utils.CustomException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flights.FlightData;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;

public class TestReadAllFlightData_Test {

	@Test
    public void testReadAllFlightDataFiles_One() throws IOException, CustomException {
		
		System.out.println("================ test one ==========================");
		train_rank_final train_rank_final_value = train_rank_final.train;
		
		FlightListData flightListData = new FlightListData(train_rank_final_value );
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
		
		Set<String> setOfFlightIds =  flightListData.getFlightListDataTable().stringColumn("flight_id").asSet();
		int index = 1;
		for ( String flight_id : setOfFlightIds) {
			System.out.println( index + " --- " + flight_id );
			
			FlightData flightData = new FlightData(train_rank_final_value , flight_id);
			flightData.readParquetWithStream();
			if ( index > 10 ) {
				break;
			}
			index++;
		}
	}
	
	@Test
    public void testReadAllFlightDataFiles_Two() throws IOException, CustomException {
		
		System.out.println("================ test two ==========================");
		train_rank_final train_rank_final_value = train_rank_final.train;
		
		FlightListData flightListData = new FlightListData(train_rank_final_value );
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
		
		SortedSet<String> listOfUniqueFlightIds = flightListData.getListOfUniqueFlightIds();
		
		int numberOfFiles = listOfUniqueFlightIds.size();
		System.out.println("---> number of flight files = " + String.valueOf(numberOfFiles));
		
		List<String> columnNamesList = Arrays.asList("latitude","longitude","altitude");

		int index = 1;
		int rowCount = 0;
		for ( String flight_id : listOfUniqueFlightIds) {
			
			System.out.println(index + " / " + String.valueOf( listOfUniqueFlightIds.size() ) + " --> " + flight_id );
			
			FlightData flightData = new FlightData(train_rank_final_value , flight_id);
			flightData.readParquetWithStream();
			
			
			Table flightDataTable = flightData.getFlightDataTable();
			//for ( String columnName : flightDataTable.columnNames() ) {
				//System.out.println(columnName);
			//}
			
			boolean shouldBreak = false;
			for ( String columnName : columnNamesList) {
				assert ( flightDataTable.columnNames().contains(columnName) );
				
				Selection selectionIsMissing = flightDataTable.doubleColumn(columnName).isMissing();
				Table filteredTable = flightDataTable.where(selectionIsMissing);
				System.out.println("for col = " + columnName + " - Missing = " + String.valueOf(filteredTable.rowCount()) + " / "  + String.valueOf(flightDataTable.rowCount()));
				if ( filteredTable.rowCount() > 0 ) {
					shouldBreak = true;
				}
			}
			
			rowCount =  rowCount + flightDataTable.rowCount();
			System.out.println ("---> cumulated row count = " + String.valueOf(rowCount));
			if ( shouldBreak == true ) {
				break;
			}
			index++;
		}
		System.out.println("---> total count of rows = " + String.valueOf(rowCount));
	}
}
