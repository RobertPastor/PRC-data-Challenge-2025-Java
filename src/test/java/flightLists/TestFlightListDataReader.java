package flightLists;

import java.io.IOException;
import java.util.Set;
import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;
import folderDiscovery.FolderDiscovery;

public class TestFlightListDataReader {

	@Test
    public void testReadFlightList() throws IOException {

		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
	}
	
	@Test
    public void testFlightListTableStructure() throws IOException {

		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
		System.out.println("structure = " + flightListData.getFlightListDataTable().structure() );
		
		FolderDiscovery  folderDiscovery = new FolderDiscovery();
		folderDiscovery.discover();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
		System.out.println("number of files = " + folderDiscovery.getFlightFolderNbFiles(train_rank.rank));
		assert flightListData.getFlightListDataTable().rowCount() == folderDiscovery.getFlightFolderNbFiles(train_rank.rank);
	}
	
	@Test
    public void testExtractListOfFlightIds() throws IOException {

		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
		
		Set<String> setOfFlightIds = flightListData.getFlightListDataTable().stringColumn("flight_id").asSet();
		int index = 1;
		for ( String flight_id : setOfFlightIds) {
			System.out.println( index + " --- " + flight_id );
			index++;
		}
	}
}
