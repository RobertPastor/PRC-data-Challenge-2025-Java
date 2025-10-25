package fuel;


import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;
import flightLists.FlightListDataTable;
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

	public void setFuelDataTable(Table fuelDataTable) {
		this.fuelDataTable = fuelDataTable;
	}

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
	
	@SuppressWarnings("deprecation")
	public void extendFuelWithFlightListData (  final Table flightListDataTable ) {
		
		// apply join using  flight_id
		// use left outer join from fuel table to flight list table
		// from flight list data use column ("flight_id")
		
		// Perform an left outer join on the "id" column
		// Left Outer Join: Keeps all rows from the left table and matches from the right.
		this.setFuelDataTable( this.fuelDataTable.joinOn("flight_id").leftOuter(flightListDataTable));
		
	}
	
	/**
	 * used during reading of the input parquet rows , hence records
	 * @param record
	 */
	public void appendRowToFuelDataTable( final FuelDataRecord record ) {

		Row row = this.fuelDataTable.appendRow();
		
		row.setInt ("idx", record.idx());
		row.setString("flight_id", record.flight_id());
		
		row.setInstant("start", record.start());
		row.setInstant("end", record.end());
		
		row.setFloat ("fuel_kg", record.fuel_kg());
	}
	
	public void extendFuelWithEndStartDifference( ) {
		
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
	
	public void extendFuelStartEndInstantswithFlightsPositions( final FlightDataTable flightDataTable ) {
		
		// find the nearest instant from a fuel table of a flight id
		// given a fuel start or stop instant
		
		
		
	}
	
	public void extendFuelStartEndInstantsWithFlightsGroundSpeeds( final FlightDataTable flightDataTable ) {
		
		
	}
}

