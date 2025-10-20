package fuelData;


import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import fuelData.FuelDataSchema.FuelDataRecord;
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
	
	public void appendRowToFuelDataTable(  FuelDataRecord r ) {

		Row row = this.fuelDataTable.appendRow();
		
		row.setInt ("idx", r.idx());
		row.setString("flight_id", r.flight_id());
		
		row.setInstant("start", r.start());
		row.setInstant("end", r.end());
		
		row.setFloat ("fuel_kg", r.fuel_kg());
	
	}
	
	public void extendWithEndStartDifference( ) {
		
		DoubleColumn time_diff_seconds = DoubleColumn.create("time_diff_seconds");
		this.fuelDataTable.addColumns(time_diff_seconds);
		
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
		System.out.println( this.fuelDataTable.print(10));
	}
}

