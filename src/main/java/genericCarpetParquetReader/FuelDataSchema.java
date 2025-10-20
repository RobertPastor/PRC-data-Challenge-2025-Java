package genericCarpetParquetReader;

import java.time.Instant;

public class FuelDataSchema {
	
	public static record FuelDataRecord(
			int idx, 
			String flight_id, 
			Instant start, 
			Instant end,
			int fuel_kg
			) {

		public int idx() {
			return idx;
		}

		    public String flight_id() {
			return flight_id;
		}

		    public Instant start() {
			return start;
		}

		    public Instant end() {
			return end;
		}

		    public int fuel_kg() {
			return fuel_kg;
		}
	}

}