package flights;

import java.time.Instant;

public class FlightDataSchema {

	public static record FlightDataRecord(
	
		String flight_id, 
		Instant timestamp,
		
		double longitude,
		double latitude,
		
		float altitude,
		float groundspeed,
		
		float track,
		float vertical_rate,
		
		float mach,
		String typecode,
		float TAS,
		float CAS,
		String source
		
	) {

		public String flight_id() {
			return flight_id;
		}

		public Instant timestamp() {
			return timestamp;
		}

		public double longitude() {
			return longitude;
		}

		public double latitude() {
			return latitude;
		}

		public float altitude() {
			return altitude;
		}

		public float groundspeed() {
			return groundspeed;
		}

		public float track() {
			return track;
		}

		public float vertical_rate() {
			return vertical_rate;
		}

		public float mach() {
			return mach;
		}

		public String typecode() {
			return typecode;
		}

		public float TAS() {
			return TAS;
		}

		public float CAS() {
			return CAS;
		}

		public String source() {
			return source;
		} }
	
}
