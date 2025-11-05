package flightLists;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;

import tech.tablesaw.api.*;

public class Test_IsAirportInRankFlightList_Test {

	
	@Test
	public  void test_IsAirportInRankFlightList () throws IOException { 
		
		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();
		
		FlightListData flightListData = new FlightListData(train_rank.rank );
		flightListData.readParquet();
		
		flightListData.extendWithAirportData(airportsData);
		System.out.println( flightListData.getFlightListDataTable().print(10));
		
		Table table = flightListData.getFlightListDataTable();

		// Filter rows where Age > 30
		Table filteredTable = table.where(table.stringColumn("origin_icao").isEqualTo("ZGOW") );
		
		filteredTable.removeColumns("flight_date","takeoff","landed","aircraft_type","origin_latitude_deg",
		           "origin_longitude_deg" ,"origin_latitude_rad","origin_longitude_rad","destination_latitude_deg" ,
		           "destination_longitude_deg","destination_latitude_rad","destination_longitude_rad",
		           "flight_distance_Nm","flight_duration_sec");
		
		 // Print the filtered table
		System.out.println(filteredTable.print(10));
		
	}

}
