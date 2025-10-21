package aircrafts;

public class AircraftsDataSchema {

	
	public static record AircraftsDataRecord(
			
			String ICAO_Code ,
			int Num_Engines,
			float Approach_Speed_knot,
			float Wingspan_ft_without_winglets_sharklets,
			float Length_ft,
			float Tail_Height_at_OEW_ft,
			float Wheelbase_ft,
			float Cockpit_to_Main_Gear_ft,
			float Main_Gear_Width_ft,
			float MTOW_lb,
			float MALW_lb,
			float Parking_Area_ft2
			) {

		public String ICAO_Code() {
			return ICAO_Code;
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

		public float MTOW_lb() {
			return MTOW_lb;
		}

		    public float MALW_lb() {
			return MALW_lb;
		}

		    public float Parking_Area_ft2() {
			return Parking_Area_ft2;
		}};
			
}
