package fuel;


import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import flights.FlightDataTable;
import fuel.FuelDataSchema.FuelDataRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

public class FuelDataTable extends Table {
 
	public Table fuelDataTable = null;

	public Table getFuelDataTable() {
		return this.fuelDataTable;
	}

	FuelDataTable() {
		super("Fuel Data");
	}
	
	public void createEmptyFuelDataTable( ) {
		this.fuelDataTable = Table.create("Fuel Data",

				IntColumn.create("idx"),
				StringColumn.create("flight_id"),

				InstantColumn.create("start"),
				InstantColumn.create("end"),

				FloatColumn.create("fuel_kg"));

	}
	
	
	
	/**
	 * used during reading of the input parquet rows , hence records
	 * @param r
	 */
	public void appendRowToFuelDataTable( FuelDataRecord r ) {

		Row row = this.fuelDataTable.appendRow();
		
		row.setInt ("idx", r.idx());
		row.setString("flight_id", r.flight_id());
		
		row.setInstant("start", r.start());
		row.setInstant("end", r.end());
		
		row.setFloat ("fuel_kg", r.fuel_kg());
	}
	
	public void extendWithEndStartDifference( ) {
		
		DoubleColumn time_diff_seconds_column = DoubleColumn.create("time_diff_seconds");
		this.fuelDataTable.addColumns(time_diff_seconds_column);
		
		System.out.println( this.fuelDataTable.structure() );
		//int maxRowCount = this.fuelDataTable.rowCount();
		
		Iterator<Row> iter = this.fuelDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			Instant start = row.getInstant("start");
			Instant end = row.getInstant("end");
			double difference = Duration.between(start, end).toSeconds();
			row.setDouble("time_diff_seconds" , difference);
		}
		System.out.println( this.fuelDataTable.print(10) );
	}
	
	/**
	 * Fuel Flow kg per seconds is the main Y to estimate (not the fuel in kg)
	 */
	public void extendFuelFlowKgSeconds() {
		
		FloatColumn fuel_flow_kg_sec_column = FloatColumn.create("fuel_flow_kg_sec");
		this.fuelDataTable.addColumns(fuel_flow_kg_sec_column);
		
		System.out.println( this.fuelDataTable.structure() );
		
		Iterator<Row> iter = this.fuelDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			Float fuel_kg = row.getFloat ("fuel_kg");
			
			Instant start = row.getInstant("start");
			Instant end = row.getInstant("end");
			long difference = Duration.between(start, end).toSeconds();

			float fuel_flow_kg_seconds = (float) 0.0;
			if ( difference > 0.0 ) {
				fuel_flow_kg_seconds = fuel_kg / difference;
			}
			row.setFloat("fuel_flow_kg_sec" , fuel_flow_kg_seconds);
		}
		System.out.println( this.fuelDataTable.print(10));
	}
	
	public void extendFuelStartEndInstantwithFlightsPositions( FlightDataTable flightDataTable ) {
		
		// find the nearest instant from a fuel table of a flight id
		// given a fuel start or stop instant
		
		
		
	}
}

