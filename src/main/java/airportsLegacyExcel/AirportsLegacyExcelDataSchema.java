package airportsLegacyExcel;

public class AirportsLegacyExcelDataSchema {

	public static record AirportDataRecord(

			//"Id","Name","Airport short name","Country","IATA","ICAO","latitude degrees","longitude degrees","elevation meters");

			int Id,
			String Name, 
			String Airport_Short_Name, 
			String Country, 
			String IATA, 
			String ICAO, 
			double airport_latitude_degrees,
			double airport_longitude_degrees,
			float airport_elevation_meters
			) {

		public int Id() {
			return Id;
		}

		public String Name() {
			return Name;
		}

		public String Airport_Short_Name() {
			return Airport_Short_Name;
		}

		public String Country() {
			return Country;
		}

		public String IATA() {
			return IATA;
		}

		public String ICAO() {
			return ICAO;
		}

		public double airport_latitude_degrees() {
			return airport_latitude_degrees;
		}

		public double airport_longitude_degrees() {
			return airport_longitude_degrees;
		}

		public float airport_elevation_meters() {
			return airport_elevation_meters;
		}

	}
}