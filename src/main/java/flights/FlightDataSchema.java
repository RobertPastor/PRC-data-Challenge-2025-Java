package flights;

import java.time.Instant;
import java.util.Optional;

import org.apache.parquet.variant.Variant;



public class FlightDataSchema {

	public static record FlightDataRecord(
	
		String flight_id, 
		Instant timestamp,
		
		Double longitude,
		Double latitude,
		
		Double altitude,
		Double groundspeed,
		
		Double track,
		Double vertical_rate,
		
		Double  mach,
		String typecode,
		
		Double TAS,
		Double CAS,
		String source
		
	) {

		public String flight_id() {
			return flight_id;
		}

		public Instant timestamp() {
			return timestamp;
		}

		public Double longitude() {
			return longitude;
		}

		public Double latitude() {
			return latitude;
		}

		public Double altitude() {
			return altitude;
		}

		public Double groundspeed() {
			return groundspeed;
		}

		public Double track() {
			return track;
		}

		public Double vertical_rate() {
			return vertical_rate;
		}

		public Double mach() {
			return mach;
		}

		public String typecode() {
			return typecode;
		}

		public Double TAS() {
			return TAS;
		}

		public Double CAS() {
			return CAS;
		}

		public String source() {
			return source;
		} }
	
}
