package flights;

import tech.tablesaw.api.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.time.Instant;
import java.util.List;
import flights.FlightDataSchema.FlightDataRecord;

public class FlightDataTable extends Table {
	
	public Table flightDataTable = null;

	public Table getFlightDataTable() {
		return this.flightDataTable;
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
	
	public boolean  flightIdIsExisting( final String flight_id ) {
		
		//InstantColumn timestampColumn = this.getFlightDataTable().instantColumn("timestamp");
		StringColumn flightIdColumn = this.getFlightDataTable().stringColumn("flight_id");
		
		Table filtered = this.getFlightDataTable().where(flightIdColumn.isEqualTo(flight_id));
		System.out.println( filtered.shape() );
				
		return ( filtered.rowCount() > 0);
	}
	
	public Instant findNearestIntantFromFlightTimeStamps( final Instant fuelInstant ) {
		
		List<Instant> listOfFlightTimeStamps = new ArrayList<Instant>();
		Iterator<Row> iter = this.flightDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			// assumption : instants in the flight records are never nulls
			listOfFlightTimeStamps.add(row.getInstant("timestamp"));
		}
		System.out.println("List of instants -> size = " + listOfFlightTimeStamps.size());
		return listOfFlightTimeStamps.stream()
                .min((i1, i2) -> Long.compare(
                        Math.abs(i1.toEpochMilli() - fuelInstant.toEpochMilli()),
                        Math.abs(i2.toEpochMilli() - fuelInstant.toEpochMilli())
                ))
                .orElse(null); // Return null if the list is empty
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
	
	/**
	 * do not provide latitude and longitudes to the model
	 * instead convert latitude and longitude using sine and cosine
	 */
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
