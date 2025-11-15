package fuel;

import java.time.Instant;

public class FuelDataSchema {

	public static record FuelExtendedDataRecord(
			
			int idx,
			String flight_id,
			
			Instant start, 
			Instant end,

			long time_diff_seconds,
			float fuel_flow_kg_sec,

			//=========================================
			// aircraft positions in latitude and longitudes
			// latitude and longitude DEGREES at fuel start
			double aircraft_latitude_deg_at_fuel_start,
			double aircraft_longitude_deg_at_fuel_start,

			// latitude longitude RADIANS at fuel start
			double aircraft_latitude_rad_at_fuel_start,
			double aircraft_longitude_rad_at_fuel_start,

			// latitude and longitude DEGREES  at fuel end
			double aircraft_latitude_deg_at_fuel_end,
			double aircraft_longitude_deg_at_fuel_end,
			
			// latitude longitude RADIANS at fuel end
			double aircraft_latitude_rad_at_fuel_end,
			double aircraft_longitude_rad_at_fuel_end,

			//==========================================
			// distances
			// flown between fuel start and fuel end
			double aircraft_distance_flown_start_end_Nm,
			double aircraft_distance_flown_origin_start_Nm,
			double aircraft_distance_flown_origin_end_Nm,
			double aircraft_distance_to_be_flown_start_destination_Nm,
			double aircraft_distance_to_be_flown_end_destination_Nm,
			
			//===================altitudes
			// altitude at fuel start and fuel stop
			double aircraft_altitude_ft_at_fuel_start,
			double aircraft_altitude_ft_at_fuel_end,

			//==================================
			// delta altitudes
			double aircraft_delta_altitude_ft_origin_fuel_start,
			double aircraft_delta_altitude_ft_origin_end_start,
			double aircraft_delta_altitude_ft_start_destination,
			double aircraft_delta_altitude_ft_end_destination,
			
			//=================================
			// ground speed
			double aircraft_groundspeed_kt_at_fuel_start,
			double aircraft_groundspeed_kt_at_fuel_end,
			
			// ground speed X and Y from cosine and sine with track angles
			double aircraft_groundspeed_kt_X_at_fuel_start,
			double aircraft_groundspeed_kt_Y_at_fuel_start,
			
			// ground speed X and Y at fuel end
			double aircraft_groundspeed_kt_X_at_fuel_end,
			double aircraft_groundspeed_kt_Y_at_fuel_end,
			
			//=================================
			// track angle DEGREES
			double aircraft_track_angle_deg_at_fuel_start,
			double aircraft_track_angle_deg_at_fuel_end,
			
			// added 27th October 2025 -RADIANS
			// track angle radians
			double aircraft_track_angle_rad_at_fuel_start,
			double aircraft_track_angle_rad_at_fuel_end,
			
			//==================================
			// vertical rate
			// computed vertical rate feet per minutes between start and end
			double aircraft_computed_vertical_rate_ft_min,
			
			//=================================
			// vertical rate
			double aircraft_vertical_rate_ft_min_at_fuel_start,
			double aircraft_vertical_rate_ft_min_at_fuel_end,
			
			//=================speeds==========
			// mach
			double aircraft_mach_at_fuel_start,
			double aircraft_mach_at_fuel_end,
			
			// TAS
			double aircraft_TAS_at_fuel_start,
			double aircraft_TAS_at_fuel_end,
			// CAS
			double aircraft_CAS_at_fuel_start,
			double aircraft_CAS_at_fuel_end,
			
			//============ airport elevations ===========
			// origin an destination airport elevation
			float origin_elevation_feet,
			float destination_elevation_feet,
			
			// flight and flight duration
			double flight_distance_Nm,			
			long flight_duration_sec,

			// using flight take-off and flight landed , compute relate duration in second
			
			long fuel_burnt_start_relative_to_takeoff_sec,
			long fuel_burnt_end_relative_to_takeoff_sec,
			long fuel_burnt_start_relative_to_landed_sec,
			long fuel_burnt_end_relative_to_landed_sec,

			// aircraft informations
			String aircraft_ICAO_Code,

			int Num_Engines,

			float Approach_Speed_knot,
			float Length_ft,
			float Wingspan_ft_without_winglets_sharklets,
			float Wingspan_ft_with_winglets_sharklets,
			float Tail_Height_at_OEW_ft,
			float Wheelbase_ft,
			float Cockpit_to_Main_Gear_ft,
			float Main_Gear_Width_ft,

			double MTOW_kg,
			double MALW_kg,
			float Parking_Area_ft2,

			// flight date data
			int flight_date_year,
			int flight_date_month,
			int flight_date_day_of_the_year

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

		public String aircraft_ICAO_Code() {
			return aircraft_ICAO_Code;
		}

		public double aircraft_distance_flown_start_end_Nm() {
			return aircraft_distance_flown_start_end_Nm;
		}

		    public double aircraft_distance_flown_origin_start_Nm() {
			return aircraft_distance_flown_origin_start_Nm;
		}

		    public double aircraft_distance_flown_origin_end_Nm() {
			return aircraft_distance_flown_origin_end_Nm;
		}

		    public double aircraft_distance_to_be_flown_start_destination_Nm() {
			return aircraft_distance_to_be_flown_start_destination_Nm;
		}

		    public double aircraft_distance_to_be_flown_end_destination_Nm() {
			return aircraft_distance_to_be_flown_end_destination_Nm;
		}

		    public double aircraft_delta_altitude_ft_origin_fuel_start() {
			return aircraft_delta_altitude_ft_origin_fuel_start;
		}

		    public double aircraft_delta_altitude_ft_origin_end_start() {
			return aircraft_delta_altitude_ft_origin_end_start;
		}

		    public double aircraft_delta_altitude_ft_start_destination() {
			return aircraft_delta_altitude_ft_start_destination;
		}

		    public double aircraft_delta_altitude_ft_end_destination() {
			return aircraft_delta_altitude_ft_end_destination;
		}

		    public long fuel_burnt_start_relative_to_landed_sec() {
			return fuel_burnt_start_relative_to_landed_sec;
		}

		    public float Wingspan_ft_without_winglets_sharklets() {
			return Wingspan_ft_without_winglets_sharklets;
		}

		    public float Wingspan_ft_with_winglets_sharklets() {
			return Wingspan_ft_with_winglets_sharklets;
		}

		public double aircraft_track_angle_radians_at_fuel_start() {
			return aircraft_track_angle_rad_at_fuel_start;
		}

		public double aircraft_track_angle_radians_at_fuel_end() {
			return aircraft_track_angle_rad_at_fuel_end;
		}

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
		
		public double aircraft_groundspeed_kt_Y_at_fuel_start() {
			return aircraft_groundspeed_kt_Y_at_fuel_start;
		}

		public double aircraft_groundspeed_kt_X_at_fuel_end() {
			return aircraft_groundspeed_kt_X_at_fuel_end;
		}


		public double aircraft_groundspeed_kt_Y_at_fuel_end() {
			return aircraft_groundspeed_kt_Y_at_fuel_end;
		}

		public double aircraft_groundspeed_kt_X_at_fuel_start() {
			return aircraft_groundspeed_kt_X_at_fuel_start;
		}


		

		public long time_diff_seconds() {
			return time_diff_seconds;
		}

		public float fuel_flow_kg_sec() {
			return fuel_flow_kg_sec;
		}

		public double aircraft_latitude_deg_at_fuel_start() {
			return aircraft_latitude_deg_at_fuel_start;
		}

		public double aircraft_longitude_deg_at_fuel_start() {
			return aircraft_longitude_deg_at_fuel_start;
		}

		public double aircraft_latitude_rad_at_fuel_start() {
			return aircraft_latitude_rad_at_fuel_start;
		}

		public double aircraft_longitude_rad_at_fuel_start() {
			return aircraft_longitude_rad_at_fuel_start;
		}

		public double aircraft_latitude_rad_at_fuel_end() {
			return aircraft_latitude_rad_at_fuel_end;
		}

		public double aircraft_longitude_rad_at_fuel_end() {
			return aircraft_longitude_rad_at_fuel_end;
		}

		public double aircraft_track_angle_rad_at_fuel_start() {
			return aircraft_track_angle_rad_at_fuel_start;
		}

		public double aircraft_track_angle_rad_at_fuel_end() {
			return aircraft_track_angle_rad_at_fuel_end;
		}

		public double aircraft_latitude_deg_at_fuel_end() {
			return aircraft_latitude_deg_at_fuel_end;
		}

		public double aircraft_longitude_deg_at_fuel_end() {
			return aircraft_longitude_deg_at_fuel_end;
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