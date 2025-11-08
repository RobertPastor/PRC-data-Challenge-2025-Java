package aircrafts;

import java.util.logging.Logger;

import aircrafts.AircraftsDataSchema.AircraftsDataRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;
import utils.Constants;

public class AircraftsDataTable extends Table {
	
	private static final Logger logger = Logger.getLogger(AircraftsDataTable.class.getName());
	
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
				StringColumn.create("aircraft_ICAO_Code") ,
				
				IntColumn.create("Num_Engines"),
				
				FloatColumn.create("Approach_Speed_knot"),
				FloatColumn.create("Wingspan_ft_without_winglets_sharklets"),
				FloatColumn.create("Wingspan_ft_with_winglets_sharklets"),
				
				FloatColumn.create("Length_ft"),
				FloatColumn.create("Tail_Height_at_OEW_ft"),
				
				FloatColumn.create("Wheelbase_ft"),
				FloatColumn.create("Cockpit_to_Main_Gear_ft"),
				
				FloatColumn.create("Main_Gear_Width_ft"),
				
				// Maximum take-off weight
				DoubleColumn.create("MTOW_lb"),
				DoubleColumn.create("MTOW_kg"),
				
				// Maximum Allowable Landing Weight (MALW)
				DoubleColumn.create("MALW_lb"),
				DoubleColumn.create("MALW_kg"),
				
				FloatColumn.create("Parking_Area_ft2"));
	}
	
	public void appendRowToAircraftsDataTable(  AircraftsDataRecord record ) {

		Row row = this.aircraftsDataTable.appendRow();

		row.setString("ICAO_Code", record.ICAO_Code());
		row.setString("aircraft_ICAO_Code", record.aircraft_ICAO_code());
		
		row.setInt("Num_Engines", record.Num_Engines());
		
		row.setFloat("Approach_Speed_knot", record.Approach_Speed_knot());
		
		row.setFloat("Wingspan_ft_without_winglets_sharklets", record.Wingspan_ft_without_winglets_sharklets());
		
		// 7th November 2025 added to discriminate aircraft with or without winglets or sharklets
		row.setFloat("Wingspan_ft_without_winglets_sharklets", record.Wingspan_ft_without_winglets_sharklets());
		
		row.setFloat("Length_ft", record.Length_ft());
		row.setFloat("Tail_Height_at_OEW_ft", record.Tail_Height_at_OEW_ft());
		
		row.setFloat("Wheelbase_ft", record.Wheelbase_ft());
		row.setFloat("Cockpit_to_Main_Gear_ft", record.Cockpit_to_Main_Gear_ft());
		
		row.setFloat("Main_Gear_Width_ft", record.Main_Gear_Width_ft());
		
		row.setDouble("MTOW_lb", record.MTOW_lb());
		row.setDouble("MTOW_kg" , record.MTOW_lb() * Constants.lbs_to_kilograms);
		
		row.setDouble("MALW_lb", record.MALW_lb());
		row.setDouble("MALW_kg", record.MALW_lb() * Constants.lbs_to_kilograms);
		
		row.setFloat("Parking_Area_ft2", record.Parking_Area_ft2());
	}
	
	/**
	 * check if a given aircraft ICAO code is in the database
	 * @param aircraftICAOcode
	 * @return
	 */
	public boolean isAircraftICAOcodeInDatabase(final String aircraftICAOcode ) {
		
		Selection selection = this.aircraftsDataTable.stringColumn("ICAO_Code").containsString(aircraftICAOcode);
		Table filteredTable = this.aircraftsDataTable.where(selection);
		
		logger.info(filteredTable.print(2));
		
		StringColumn first = filteredTable.stringColumn("ICAO_Code").first(1);
		logger.info( String.valueOf(( first != null ) && !first.isEmpty() ) );
		return ( ( first != null ) && !first.isEmpty() );
		
	}
}
