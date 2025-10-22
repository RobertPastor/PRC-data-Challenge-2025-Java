package flightData;

import tech.tablesaw.api.*;

import java.util.Iterator;

import flightData.FlightDataSchema.FlightDataRecord;

public class FlightDataTable extends Table {
	
	public Table flightDataTable = null;

	public Table getFlightDataTable() {
		return flightDataTable;
	}
	
	FlightDataTable() {
		super("FlightData");
	}

	public void createEmptyFlightDataTable( ) {
		this.flightDataTable = Table.create("Flight Data",
                StringColumn.create("flight_id"),
                InstantColumn.create("timestamp"),
                
                DoubleColumn.create("longitude"),
                DoubleColumn.create("latitude"),
                
                FloatColumn.create("altitude"),
                FloatColumn.create("groundspeed"),
                FloatColumn.create("track"),
                FloatColumn.create("vertical_rate"),
                FloatColumn.create("mach"),
                StringColumn.create("typecode"),
                FloatColumn.create("TAS"),
                FloatColumn.create("CAS"),
				StringColumn.create("source"));
		
	}
	
	public void appendRowToFlightDataTable(  FlightDataRecord r ) {
		
		Row row = this.flightDataTable.appendRow();
        row.setString("flight_id", r.flight_id());
        row.setInstant("timestamp", r.timestamp());
        
        row.setDouble("longitude", r.longitude());
        row.setDouble("latitude", r.latitude());
        
        row.setFloat("altitude", r.altitude());
        
        row.setFloat("groundspeed", r.groundspeed());
        row.setFloat("vertical_rate", r.vertical_rate());
        row.setFloat("mach", r.mach());
        row.setString("typecode", r.typecode());
        row.setFloat("TAS", r.TAS());
        row.setFloat("CAS", r.CAS());
        row.setString("source", r.source());
	}
	
	
	public void extendWithLatitudeLongitudeCosineSine( ) {
	
		DoubleColumn latitude_cosine_column = DoubleColumn.create("latitude_cosine");
		this.flightDataTable.addColumns(latitude_cosine_column);
		
		DoubleColumn latitude_sine_column = DoubleColumn.create("latitude_sine");
		this.flightDataTable.addColumns(latitude_sine_column);
	
		DoubleColumn longitude_cosine_column = DoubleColumn.create("longitude_cosine");
		this.flightDataTable.addColumns(longitude_cosine_column);
	
		DoubleColumn longitude_sine_column = DoubleColumn.create("longitude_sine");
		this.flightDataTable.addColumns(longitude_sine_column);
		
		System.out.println( this.flightDataTable.structure() );
		
		Iterator<Row> iter = this.flightDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			
			double origin_latitude_degrees = row.getDouble("latitude");
			double origin_longitude_degrees = row.getDouble("longitude");
			
			row.setDouble ("latitude_cosine" , Math.cos(Math.toRadians(origin_latitude_degrees)));
			row.setDouble("latitude_sine" , Math.sin(Math.toRadians(origin_latitude_degrees)));
			
			row.setDouble("longitude_cosine" , Math.cos(Math.toRadians(origin_longitude_degrees)));
			row.setDouble("longitude_sine" , Math.sin(Math.toRadians(origin_longitude_degrees)));

		}

 	}
}
