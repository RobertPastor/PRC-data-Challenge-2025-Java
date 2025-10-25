package fuel;

import java.time.Instant;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.LongColumn;

public class FuelDataSchema {
	
	public static record FuelExtendedDataRecord(
			int idx,
			String flight_id, 
			Instant start, 
			Instant end,
			
			double time_diff_seconds,
			float fuel_flow_kg_sec,
			
			// latitude and longitude at fuel start
			double aircraft_latitude_at_fuel_start,
			double aircraft_longitude_at_fuel_start,
			
			// latitude and longitude at fuel end
			double aircraft_latitude_at_fuel_end,
			double aircraft_longitude_at_fuel_end,
			
			// distance flown between fuel start and fuel end
			double aircraft_distance_flown_Nm,
			
			// altitude at fuel start and fuel stop
			float aircraft_altitude_ft_at_fuel_start,
			float aircraft_altitude_ft_at_fuel_end,
			
			// computed vertical rate feet per minutes
			float aircraft_computed_vertical_rate_ft_min,
			
			float aircraft_groundspeed_kt_at_fuel_start,
			float aircraft_groundspeed_kt_at_fuel_end,
			
			float aircraft_track_angle_deg_at_fuel_start,
			float aircraft_track_angle_deg_at_fuel_end,
			
			float aircraft_vertical_rate_ft_min_at_fuel_start,
			float aircraft_vertical_rate_ft_min_at_fuel_end,
			
			float origin_elevation_feet,
			float destination_elevation_feet,
			
			double flight_distance_Nm,			
			long flight_duration_sec
	
			
			) {
		
		public double flight_distance_Nm() {
			return flight_distance_Nm;
		}

		public long flight_duration_sec() {
			return flight_duration_sec;
		}

		public float origin_elevation_feet() {
			return origin_elevation_feet;
		}

		    public float destination_elevation_feet() {
			return destination_elevation_feet;
		}

		public float aircraft_computed_vertical_rate_ft_min() {
			return aircraft_computed_vertical_rate_ft_min;
		}

		public double aircraft_distance_flown_Nm() {
			return aircraft_distance_flown_Nm;
		}

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
		
		public double time_diff_seconds() {
			return time_diff_seconds;
		}

		    public float fuel_flow_kg_sec() {
			return fuel_flow_kg_sec;
		}

		    public double aircraft_latitude_at_fuel_start() {
			return aircraft_latitude_at_fuel_start;
		}

		    public double aircraft_longitude_at_fuel_start() {
			return aircraft_longitude_at_fuel_start;
		}

		    public double aircraft_latitude_at_fuel_end() {
			return aircraft_latitude_at_fuel_end;
		}

		    public double aircraft_longitude_at_fuel_end() {
			return aircraft_longitude_at_fuel_end;
		}

		    public float aircraft_altitude_ft_at_fuel_start() {
			return aircraft_altitude_ft_at_fuel_start;
		}

		    public float aircraft_altitude_ft_at_fuel_end() {
			return aircraft_altitude_ft_at_fuel_end;
		}

		    public float aircraft_groundspeed_kt_at_fuel_start() {
			return aircraft_groundspeed_kt_at_fuel_start;
		}

		    public float aircraft_groundspeed_kt_at_fuel_end() {
			return aircraft_groundspeed_kt_at_fuel_end;
		}

		    public float aircraft_track_angle_deg_at_fuel_start() {
			return aircraft_track_angle_deg_at_fuel_start;
		}

		    public float aircraft_track_angle_deg_at_fuel_end() {
			return aircraft_track_angle_deg_at_fuel_end;
		}

		    public float aircraft_vertical_rate_ft_min_at_fuel_start() {
			return aircraft_vertical_rate_ft_min_at_fuel_start;
		}

		    public float aircraft_vertical_rate_ft_min_at_fuel_end() {
			return aircraft_vertical_rate_ft_min_at_fuel_end;
		}
	}
	
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