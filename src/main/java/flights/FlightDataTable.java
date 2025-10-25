package flights;

import tech.tablesaw.api.*;
import tech.tablesaw.selection.Selection;

import java.util.ArrayList;
import java.util.Iterator;
import java.time.Instant;
import java.util.List;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import flights.FlightDataSchema.FlightDataRecord;

public class FlightDataTable extends Table {
	
	protected train_rank train_rank_value;
	
	public train_rank getTrain_rank_value() {
		return train_rank_value;
	}
	
	protected String flight_id = "";
	
	public void setTrain_rank_value(train_rank train_rank_value) {
		this.train_rank_value = train_rank_value;
	}

	public void setFlight_id(String flight_id) {
		this.flight_id = flight_id;
	}

	public void setFlightDataTable(Table flightDataTable) {
		this.flightDataTable = flightDataTable;
	}

	public String getFlight_id() {
		return flight_id;
	}
	
	public Table flightDataTable = null;

	public Table getFlightDataTable() {
		return this.flightDataTable;
	}
	
	protected FlightDataTable(final train_rank train_rank_value , final String flight_id) {
		super("Flight Data");
		this.setFlight_id(flight_id);
		this.setTrain_rank_value(train_rank_value);
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
	
	public double getDoubleFlightDataAtNearestFuelInstant(final String columnName , Instant start_end) {
		Instant nearestInstand = this.findNearestIntantFromFlightTimeStamps(start_end);
		
		Selection selection = this.flightDataTable.instantColumn("timestamp").isEqualTo(nearestInstand);
		Table filtered = this.flightDataTable.where(selection);
		
		//assert filtered.rowCount() == 1;
		if ( filtered.rowCount()  >= 1) {
			return filtered.doubleColumn(columnName).get(0);
		} else {
			return (double)0.0;
		}
	}
	
	public float getFloatFlightDataAtNearestFuelInstant(final String columnName , Instant start_end) {
		Instant nearestInstand = this.findNearestIntantFromFlightTimeStamps(start_end);
		
		Selection selection = this.flightDataTable.instantColumn("timestamp").isEqualTo(nearestInstand);
		Table filtered = this.flightDataTable.where(selection);
		
		//assert filtered.rowCount() == 1;
		if ( filtered.rowCount() >= 1) {
			return filtered.floatColumn(columnName).get(0) == null ? (float)0.0 : filtered.floatColumn(columnName).get(0);
		} else {
			return (float) 0.0;
		}
	}
	
	
	public Instant findNearestIntantFromFlightTimeStamps( final Instant fuelInstant ) {
		
		List<Instant> listOfFlightTimeStamps = new ArrayList<Instant>();
		Iterator<Row> iter = this.flightDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			// assumption : instants in the flight records are never nulls
			listOfFlightTimeStamps.add(row.getInstant("timestamp"));
		}
		//System.out.println("List of instants -> size = " + listOfFlightTimeStamps.size());
		Instant nearestInstantFound = listOfFlightTimeStamps.stream()
                .min((i1, i2) -> Long.compare(
                        Math.abs(i1.toEpochMilli() - fuelInstant.toEpochMilli()),
                        Math.abs(i2.toEpochMilli() - fuelInstant.toEpochMilli())
                ))
                .orElse(null); // Return null if the list is empty
		//System.out.println("nearest found = " + nearestInstantFound);
		return nearestInstantFound;
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
	 * 
	 * warning range of latitude is Latitude:
	 * Values range from -90° to +90°.
	 * Lines of latitude run east-west and are parallel to each other.
	 * The equator is at 0° latitude.
	 * 
	 * Longitude:
	 * Values range from -180° to +180°.
	 * Lines of longitude run north-south and converge at the poles.
	 * The prime meridian (0° longitude) runs through Greenwich, England.
	 * These ranges define the geographic coordinate system used to specify locations on Earth.
	 * 
	 * same function to compute latitude and longitude of airports location
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
			//@TODO
			// need to change value range to 0.0 360.0 for cosinus and sinus to work without discontinuity
			row.setDouble ("latitude_cosine" , Math.cos(Math.toRadians(origin_latitude_degrees)));
			row.setDouble("latitude_sine" , Math.sin(Math.toRadians(origin_latitude_degrees)));
			
			row.setDouble("longitude_cosine" , Math.cos(Math.toRadians(origin_longitude_degrees)));
			row.setDouble("longitude_sine" , Math.sin(Math.toRadians(origin_longitude_degrees)));

		}

 	}
}
