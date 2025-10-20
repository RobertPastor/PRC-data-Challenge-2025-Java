package fuelData;

import java.time.Instant;

public class FuelDataSchema {
	
	public static record FuelDataRecord(
			int idx, 
			String flight_id, 
			Instant start, 
			Instant end,
			float fuel_kg
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

		    public float fuel_kg() {
			return fuel_kg;
		}
	}

}