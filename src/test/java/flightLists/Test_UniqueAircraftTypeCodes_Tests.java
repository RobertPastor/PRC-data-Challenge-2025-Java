package flightLists;

import java.io.IOException;
import java.util.SortedSet;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;

public class Test_UniqueAircraftTypeCodes_Tests {

	@Test
	public  void test_ListUniqueAircraftIACOcodes () throws IOException { 
		
		System.out.println("==================== test one ====================");
		
		FlightListData trainFlightListData = new FlightListData(train_rank_final.train );
		trainFlightListData.readParquet();
		
		//=================================== 
		
		FlightListData rankFlightListData = new FlightListData(train_rank_final.rank );
		rankFlightListData.readParquet();
		
		FlightListData finalFlightListData = new FlightListData(train_rank_final.final_submission );
		finalFlightListData.readParquet();
		
		System.out.println("size of aircrafts from RANK flight list = " + String.valueOf(rankFlightListData.getListOfUniqueAircraftICAOTypes().size()));
		System.out.println("size of aircrafts from FINAL flight list = " + String.valueOf(finalFlightListData.getListOfUniqueAircraftICAOTypes().size()));
		
		SortedSet<String> uniqueAircraftIcaoCodesListRankFinal = (SortedSet<String>) rankFlightListData.getListOfUniqueAircraftICAOTypes();
		uniqueAircraftIcaoCodesListRankFinal.addAll( finalFlightListData.getListOfUniqueAircraftICAOTypes() );
		
		System.out.println("size of aircrafts from RANK and FINAL flight list = " + String.valueOf(uniqueAircraftIcaoCodesListRankFinal.size() ) );

		for ( String aircraftIcaoCode : uniqueAircraftIcaoCodesListRankFinal) {
			System.out.println("aircraft ICAO code in Rank and final = " + aircraftIcaoCode);
		}
		
		// find all aircrafts in train that are not in rank nor in final
		for ( String aircraftICAOcodeTrain : trainFlightListData.getListOfUniqueAircraftICAOTypes()) {
			if ( uniqueAircraftIcaoCodesListRankFinal.contains(aircraftICAOcodeTrain) == false ) {
				
				System.out.println("====> aircraft from TRAIN not in Rank and Final = "  + aircraftICAOcodeTrain);
			}
		}
	}
	
	@Test
	public  void test_ListUniqueAircraftIACOcodesRankFinal () throws IOException { 
		
		System.out.println("==================== test two ====================");

		FlightListData rankFlightListData = new FlightListData( train_rank_final.rank );
		rankFlightListData.readParquet();
		
		SortedSet<String> uniqueAircraftIcaoCodesListRankFinal = rankFlightListData.getListOfUniqueAircraftICAOTypes();
		System.out.println("size of aircrafts from RANK flight list = " + String.valueOf(rankFlightListData.getListOfUniqueAircraftICAOTypes().size()));

		FlightListData finalFlightListData = new FlightListData( train_rank_final.final_submission );
		finalFlightListData.readParquet();
		
		uniqueAircraftIcaoCodesListRankFinal.addAll( finalFlightListData.getListOfUniqueAircraftICAOTypes() );
		
		System.out.println("size of aircrafts from FINAL flight list = " + String.valueOf(finalFlightListData.getListOfUniqueAircraftICAOTypes().size()));

		System.out.println("size of aircrafts from RANK and FINAL flight list = " + String.valueOf(uniqueAircraftIcaoCodesListRankFinal.size()));

		for ( String aircraftIcaoCode : uniqueAircraftIcaoCodesListRankFinal) {
			System.out.println( aircraftIcaoCode);
		}
	}
}
