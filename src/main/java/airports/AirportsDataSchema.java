package airports;

public class AirportsDataSchema {

	public static record AirportDataRecord(
			
			String icao, 
			float longitude,
			float latitude,
			float elevation
			) {

		public String icao() {
			return icao;
		}
		
		public float longitude() {
			return longitude;
		}

		public float latitude() {
			return latitude;
		}

		public float elevation() {
			return elevation;
		}
	}
}
