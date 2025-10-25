package aircrafts;

import aircrafts.AircraftsDataSchema.AircraftsDataRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import utils.Constants;

public class AircraftsDataTable extends Table {
	
	public Table aircraftsDataTable = null;

	public Table getAircraftDataTable() {
		return aircraftsDataTable;
	}

	protected AircraftsDataTable() {
		super("Aircrafts Data");
	}

	public void createEmptyAircraftsDataTable( ) {
		this.aircraftsDataTable = Table.create("Aircrafts Data",
				
				StringColumn.create("ICAO_Code") ,
				IntColumn.create("Num_Engines"),
				
				FloatColumn.create("Approach_Speed_knot"),
				FloatColumn.create("Wingspan_ft_without_winglets_sharklets"),
				
				FloatColumn.create("Length_ft"),
				FloatColumn.create("Tail_Height_at_OEW_ft"),
				
				FloatColumn.create("Wheelbase_ft"),
				FloatColumn.create("Cockpit_to_Main_Gear_ft"),
				
				FloatColumn.create("Main_Gear_Width_ft"),
				
				// Maximum takeoff weight
				DoubleColumn.create("MTOW_lb"),
				DoubleColumn.create("MTOW_kg"),
				
				// Maximum Allowable Landing Weight (MALW)
				DoubleColumn.create("MALW_lb"),
				DoubleColumn.create("MALW_kg"),
				
				FloatColumn.create("Parking_Area_ft2"));
	}
	
	public void appendRowToAircraftsDataTable(  AircraftsDataRecord r ) {

		Row row = this.aircraftsDataTable.appendRow();

		row.setString("ICAO_Code", r.ICAO_Code());
		row.setInt("Num_Engines", r.Num_Engines());
		
		row.setFloat("Approach_Speed_knot", r.Approach_Speed_knot());
		row.setFloat("Wingspan_ft_without_winglets_sharklets", r.Wingspan_ft_without_winglets_sharklets());
		
		row.setFloat("Length_ft", r.Length_ft());
		row.setFloat("Tail_Height_at_OEW_ft", r.Tail_Height_at_OEW_ft());
		
		row.setFloat("Wheelbase_ft", r.Wheelbase_ft());
		row.setFloat("Cockpit_to_Main_Gear_ft", r.Cockpit_to_Main_Gear_ft());
		
		row.setFloat("Main_Gear_Width_ft", r.Main_Gear_Width_ft());
		
		row.setDouble("MTOW_lb", r.MTOW_lb());
		row.setDouble("MTOW_kg" , r.MTOW_lb() * Constants.lbs_to_kilograms);
		
		row.setDouble("MALW_lb", r.MALW_lb());
		row.setDouble("MALW_kg", r.MALW_lb() * Constants.lbs_to_kilograms);
		
		row.setFloat("Parking_Area_ft2", r.Parking_Area_ft2());

	}
	
}
