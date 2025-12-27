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

public class Test_SearchForMissingFeaturesInInterpolated_Tests {
	
	public void searchForMissingFeatures( final train_rank_final train_rank_final_value, 
			final List<String> listOfUniqueFlightIds) throws IOException, CustomException {
		
		SortedSet<String> flightIdsWithMissingData = new TreeSet<String>();

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
			
			for ( String columnName : columnNamesList) {
				assert ( flightDataTable.columnNames().contains(columnName) );
				
				Selection selectionIsMissing = flightDataTable.doubleColumn(columnName).isMissing();
				Table filteredTable = flightDataTable.where(selectionIsMissing);
				//System.out.println("for col = " + columnName + " - Missing = " + String.valueOf(filteredTable.rowCount()) + " / "  + String.valueOf(flightDataTable.rowCount()));
				if ( filteredTable.rowCount() > 0 ) {
					flightIdsWithMissingData.add(flight_id);
				}
			}
			
			System.out.println ("---> index = " + String.valueOf(index));
			
			index++;
		}
		System.out.println("---> total count of rows = " + String.valueOf(rowCount));
		
		for (String flight_id : flightIdsWithMissingData) {
			System.out.println(flight_id);
		}
	}
	
	
	@Test
    public void searchForMissingFeatures_Train() throws IOException, CustomException {
		
		
		train_rank_final train_rank_final_value = train_rank_final.train;
		
		List<String> flightIdsTrainList = List.of("prc770822360","prc770885136","prc770887555",
				"prc770893597", "prc772539375" ,"prc776853928", "prc777326263","prc784305329");

		String aircraft_type_code = "A320";

		FlightListData flightListData = new FlightListData(train_rank_final_value , aircraft_type_code);
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
				
		SortedSet<String> listOfUniqueFlightIds = flightListData.getListOfUniqueFlightIds();
		
		int numberOfFiles = listOfUniqueFlightIds.size();
		System.out.println("---> number of flight files = " + String.valueOf(numberOfFiles));

		
		this.searchForMissingFeatures( train_rank_final_value , flightIdsTrainList );

	}
	
	@Test
    public void searchForMissingFeatures_Rank() throws IOException, CustomException {
		
		train_rank_final train_rank_final_value = train_rank_final.rank;

		List<String> flightIdsRankList = List.of("prc806642601","prc806714985","prc809272840");
		
		String aircraft_type_code = "A320";
		
		FlightListData flightListData = new FlightListData(train_rank_final_value , aircraft_type_code);
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );

		SortedSet<String> listOfUniqueFlightIds = flightListData.getListOfUniqueFlightIds();
		
		int numberOfFiles = listOfUniqueFlightIds.size();
		System.out.println("---> number of flight files = " + String.valueOf(numberOfFiles));

		this.searchForMissingFeatures( train_rank_final_value , flightIdsRankList);

	}
	
	@Test
    public void searchForMissingFeatures_Final() throws IOException, CustomException {
		
		train_rank_final train_rank_final_value = train_rank_final.final_submission;

		List<String> flightIdsFinalList = List.of("prc814330043","prc814386688","prc814513223","prc815137423","prc820157283");

		FlightListData flightListData = new FlightListData(train_rank_final_value );
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );

		this.searchForMissingFeatures( train_rank_final_value , flightIdsFinalList);

	}
}
