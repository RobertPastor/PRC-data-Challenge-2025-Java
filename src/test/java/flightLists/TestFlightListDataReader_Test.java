package flightLists;

import java.io.IOException;
import java.util.Set;
import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import folderDiscovery.FolderDiscovery;

public class TestFlightListDataReader_Test {

	@Test
    public void testReadTrainFlightList() throws IOException {

		final train_rank_final train_rank_value = train_rank_final.train;
		FlightListData flightListData = new FlightListData( train_rank_value  );
		flightListData.readParquet();
	}
	
	@Test
    public void testReadRankFlightList() throws IOException {

		final train_rank_final train_rank_value = train_rank_final.rank;
		FlightListData flightListData = new FlightListData(train_rank_value  );
		flightListData.readParquet();
	}
	
	@Test
    public void testFlightListTableStructure() throws IOException {

		final train_rank_final train_rank_value = train_rank_final.rank;
		FlightListData flightListData = new FlightListData( train_rank_value );
		flightListData.readParquet();
		System.out.println("structure = " + flightListData.getFlightListDataTable().structure() );
		
		FolderDiscovery  folderDiscovery = new FolderDiscovery();
		folderDiscovery.discover();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
		System.out.println("number of files = " + folderDiscovery.getFlightFolderNbFiles(train_rank_final.rank));
		assert flightListData.getFlightListDataTable().rowCount() == folderDiscovery.getFlightFolderNbFiles(train_rank_final.rank);
	}
	
	@Test
    public void testExtractListOfFlightIds() throws IOException {

		FlightListData flightListData = new FlightListData(train_rank_final.rank );
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
