package flightListData;

import java.time.Instant;
import java.time.LocalDate;

public class FlightListDataSchema {

	public static record FlightListDataRecord(
			
			String flight_id, 
			LocalDate flight_date,
			Instant takeoff,
			String origin_icao,
			String origin_name,
			Instant landed,
			String destination_icao,
			String destination_name,
			String aircraft_type
			) { }
			
}
