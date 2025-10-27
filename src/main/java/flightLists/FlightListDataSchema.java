package flightLists;

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
			) {

		public String flight_id() {
			return flight_id;
		}

		public LocalDate flight_date() {
			return flight_date;
		}

		public Instant takeoff() {
			return takeoff;
		}

		public String origin_icao() {
			return origin_icao;
		}

		public String origin_name() {
			return origin_name;
		}

		public Instant landed() {
			return landed;
		}

		public String destination_icao() {
			return destination_icao;
		}

		public String destination_name() {
			return destination_name;
		}

		public String aircraft_type() {
			return aircraft_type;
		}
	}
}
