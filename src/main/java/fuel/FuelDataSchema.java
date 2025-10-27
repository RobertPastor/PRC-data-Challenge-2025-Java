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

			long time_diff_seconds,
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
			double aircraft_altitude_ft_at_fuel_start,
			double aircraft_altitude_ft_at_fuel_end,

			// computed vertical rate feet per minutes
			double aircraft_computed_vertical_rate_ft_min,
			
			// ground speed
			double aircraft_groundspeed_kt_at_fuel_start,
			double aircraft_groundspeed_kt_at_fuel_end,

			double aircraft_track_angle_deg_at_fuel_start,
			double aircraft_track_angle_deg_at_fuel_end,

			double aircraft_vertical_rate_ft_min_at_fuel_start,
			double aircraft_vertical_rate_ft_min_at_fuel_end,
			
			double aircraft_mach_at_fuel_start,
			double aircraft_mach_at_fuel_end,

			double aircraft_TAS_at_fuel_start,
			double aircraft_TAS_at_fuel_end,

			double aircraft_CAS_at_fuel_start,
			double aircraft_CAS_at_fuel_end,

			float origin_elevation_feet,
			float destination_elevation_feet,

			double flight_distance_Nm,			
			long flight_duration_sec,

			// using flight take-off and flight landed , compute relate duration in second
			long fuel_burnt_start_relative_to_takeoff_sec,
			long fuel_burnt_end_relative_to_takeoff_sec,
			long fuel_burnt_end_relative_to_landed_sec,

			int Num_Engines,

			float Approach_Speed_knot,
			float Wingspan_ft_without_winglets_sharklets,
			float Length_ft,
			float Tail_Height_at_OEW_ft,
			float Wheelbase_ft,
			float Cockpit_to_Main_Gear_ft,
			float Main_Gear_Width_ft,

			double MTOW_kg,
			double MALW_kg,
			float Parking_Area_ft2,

			int flight_date_year,
			int flight_date_month,
			int flight_date_day_of_the_year

			) {

		public double aircraft_mach_at_fuel_start() {
			return aircraft_mach_at_fuel_start;
		}

		    public double aircraft_mach_at_fuel_end() {
			return aircraft_mach_at_fuel_end;
		}

		    public double aircraft_TAS_at_fuel_start() {
			return aircraft_TAS_at_fuel_start;
		}

		    public double aircraft_TAS_at_fuel_end() {
			return aircraft_TAS_at_fuel_end;
		}

		    public double aircraft_CAS_at_fuel_start() {
			return aircraft_CAS_at_fuel_start;
		}

		    public double aircraft_CAS_at_fuel_end() {
			return aircraft_CAS_at_fuel_end;
		}

		public int flight_date_year() {
			return flight_date_year;
		}

		public int flight_date_month() {
			return flight_date_month;
		}

		public int flight_date_day_of_the_year() {
			return flight_date_day_of_the_year;
		}

		public int Num_Engines() {
			return Num_Engines;
		}

		public float Approach_Speed_knot() {
			return Approach_Speed_knot;
		}

		public float Wingspan_ft_without_winglets_sharklets() {
			return Wingspan_ft_without_winglets_sharklets;
		}

		public float Length_ft() {
			return Length_ft;
		}

		public float Tail_Height_at_OEW_ft() {
			return Tail_Height_at_OEW_ft;
		}

		public float Wheelbase_ft() {
			return Wheelbase_ft;
		}

		public float Cockpit_to_Main_Gear_ft() {
			return Cockpit_to_Main_Gear_ft;
		}

		public float Main_Gear_Width_ft() {
			return Main_Gear_Width_ft;
		}

		public double MTOW_kg() {
			return MTOW_kg;
		}

		public double MALW_kg() {
			return MALW_kg;
		}

		public float Parking_Area_ft2() {
			return Parking_Area_ft2;
		}

		public long fuel_burnt_start_relative_to_takeoff_sec() {
			return fuel_burnt_start_relative_to_takeoff_sec;
		}

		public long fuel_burnt_end_relative_to_takeoff_sec() {
			return fuel_burnt_end_relative_to_takeoff_sec;
		}

		public long fuel_burnt_end_relative_to_landed_sec() {
			return fuel_burnt_end_relative_to_landed_sec;
		}

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

		public long time_diff_seconds() {
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

		public double aircraft_altitude_ft_at_fuel_start() {
			return aircraft_altitude_ft_at_fuel_start;
		}

		public double aircraft_altitude_ft_at_fuel_end() {
			return aircraft_altitude_ft_at_fuel_end;
		}

		public double aircraft_groundspeed_kt_at_fuel_start() {
			return aircraft_groundspeed_kt_at_fuel_start;
		}

		public double aircraft_groundspeed_kt_at_fuel_end() {
			return aircraft_groundspeed_kt_at_fuel_end;
		}

		public double aircraft_track_angle_deg_at_fuel_start() {
			return aircraft_track_angle_deg_at_fuel_start;
		}

		public double aircraft_track_angle_deg_at_fuel_end() {
			return aircraft_track_angle_deg_at_fuel_end;
		}

		public double aircraft_vertical_rate_ft_min_at_fuel_start() {
			return aircraft_vertical_rate_ft_min_at_fuel_start;
		}

		public double aircraft_vertical_rate_ft_min_at_fuel_end() {
			return aircraft_vertical_rate_ft_min_at_fuel_end;
		}
		

		public double aircraft_computed_vertical_rate_ft_min() {
			return aircraft_computed_vertical_rate_ft_min;
		}

		public double aircraft_distance_flown_Nm() {
			return aircraft_distance_flown_Nm;
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