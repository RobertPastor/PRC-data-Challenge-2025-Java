package flights;

import tech.tablesaw.api.*;
import tech.tablesaw.selection.Selection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import flights.FlightDataSchema.FlightDataRecord;

public class FlightDataTable extends Table {
	
	private static final Logger logger = Logger.getLogger(FlightDataTable.class.getName());

	
	protected train_rank train_rank_value;
	
	public train_rank getTrain_rank_value() {
		return train_rank_value;
	}
	
	// map of interpolation functions for each interpolated column
	protected Map<String, PolynomialSplineFunction> interpolationFunctionMap = null;

	public Map<String, PolynomialSplineFunction> getInterpolationFunctionMap() {
		return interpolationFunctionMap;
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
	// constructor
	protected FlightDataTable(final train_rank train_rank_value , final String flight_id) {
		super("Flight Data");
		this.setFlight_id(flight_id);
		this.setTrain_rank_value(train_rank_value);
		// map of interpolated function
		interpolationFunctionMap = new HashMap<>();
	}

	public void createEmptyFlightDataTable( ) {
		this.flightDataTable = Table.create("Flight Data",
                StringColumn.create("flight_id"),
                InstantColumn.create("timestamp"),
                
                DoubleColumn.create("longitude"),
                DoubleColumn.create("latitude"),
                
                DoubleColumn.create("altitude"),
                DoubleColumn.create("groundspeed"),
                
                DoubleColumn.create("track"),
                
                DoubleColumn.create("vertical_rate"),
                DoubleColumn.create("mach"),
                
                StringColumn.create("typecode"),
                
                DoubleColumn.create("TAS"),
                DoubleColumn.create("CAS"),
				StringColumn.create("source"));
		
	}
	
	public boolean  flightIdIsExisting( final String flight_id ) {
		
		//InstantColumn timestampColumn = this.getFlightDataTable().instantColumn("timestamp");
		StringColumn flightIdColumn = this.getFlightDataTable().stringColumn("flight_id");
		
		Table filtered = this.getFlightDataTable().where(flightIdColumn.isEqualTo(flight_id));
		System.out.println( filtered.shape() );
				
		return ( filtered.rowCount() > 0);
	}
	
	public double getDoubleFlightDataAtInterpolatedStartEndFuelInstant(final String columnName , final Instant start_end) {
				
		// use interpolation function when start end instant inside the range of the function
		// 	private Map<String, PolynomialSplineFunction> interpolationFunctionMap = null;
		
		if ( interpolationFunctionMap.containsKey(columnName)) {
			logger.info("Interpolation map contains an interpolation function for the column = " + columnName);
		} else {
			assert (false);
		}

		PolynomialSplineFunction function = (PolynomialSplineFunction)interpolationFunctionMap.get(columnName);
		double querySeconds = start_end.getEpochSecond();

		if ( )
        double interpolatedValue = function.value(querySeconds);
        return interpolatedValue;
		
	}
	
	public float getFloatFlightDataAtInterpolatedStartEndFuelInstant(final String columnName , final Instant start_end) {
		
		if ( interpolationFunctionMap.containsKey(columnName)) {
			logger.info("Interpolation map contains an interpolation function for the column = " + columnName);
		} else {
			assert (false);
		}
		
		PolynomialSplineFunction function = (PolynomialSplineFunction)interpolationFunctionMap.get(columnName);
		double querySeconds = start_end.getEpochSecond();

        double interpolatedValue = function.value(querySeconds);
        return (float)interpolatedValue;
	}
	
	
	public Instant interpolateFromFuelStartEnd( final Instant fuelInstant ) {
		
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
	
	public void appendRowToFlightDataTable(  FlightDataRecord record ) {
		
		Row row = this.flightDataTable.appendRow();
        row.setString("flight_id", record.flight_id());
        row.setInstant("timestamp", record.timestamp());
        
        row.setDouble("longitude", record.longitude());
        row.setDouble("latitude", record.latitude());
        
        row.setDouble("altitude", record.altitude());
        
        row.setDouble("groundspeed", record.groundspeed());
        row.setDouble("vertical_rate", record.vertical_rate());
        row.setDouble("mach", record.mach());
        
        row.setString("typecode", record.typecode());
        
        row.setDouble("TAS", record.TAS());
        row.setDouble("CAS", record.CAS());
        
        row.setString("source", record.source());
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
