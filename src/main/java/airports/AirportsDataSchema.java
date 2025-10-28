package airports;

public class AirportsDataSchema {

	public static record AirportDataRecord(
			
			String icao, 
			double longitude,
			double latitude,
			double elevation
			) {

		public String icao() {
			return icao;
		}
		
		public double longitude() {
			return longitude;
		}

		public double latitude() {
			return latitude;
		}

		public double elevation() {
			return elevation;
		}
	}
}
