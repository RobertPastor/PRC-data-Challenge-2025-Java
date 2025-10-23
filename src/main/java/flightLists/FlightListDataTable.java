package flightLists;

import java.util.Iterator;

import airports.AirportsDataTable;
import flightLists.FlightListDataSchema.FlightListDataRecord;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import utils.Utils;

import java.time.Duration;
import java.time.Instant;

public class FlightListDataTable extends Table {
	
	
	public Table flightListDataTable = null;

	public Table getFlightListDataTable() {
		return flightListDataTable;
	}

	FlightListDataTable() {
		super("FlightListData");
	}

	public void createEmptyFlightListDataTable( ) {
		this.flightListDataTable = Table.create("Flight List Data",

				StringColumn.create("flight_id"),
				DateColumn.create("flight_date"),

				InstantColumn.create("takeoff"),

				StringColumn.create("origin_icao"),
				StringColumn.create("origin_name"),

				InstantColumn.create("landed"),

				StringColumn.create("destination_icao"),
				StringColumn.create("destination_name"),

				StringColumn.create("aircraft_type"));

	}

	public void appendRowToFlightListDataTable(  FlightListDataRecord r ) {

		Row row = this.flightListDataTable.appendRow();
		
		row.setString("flight_id", r.flight_id());
		row.setDate("flight_date", r.flight_date());
		
		row.setInstant("takeoff", r.takeoff());
		
		row.setString("origin_icao", r.origin_icao());
		row.setString("origin_name", r.origin_name());
		
		row.setInstant("landed", r.landed());

		row.setString("destination_icao", r.destination_icao());
		row.setString("destination_name", r.destination_name());

		row.setString("aircraft_type", r.aircraft_type());
	}
	
	/*
	 * origin_longitude |   origin_latitude |   origin_elevation |   destination_longitude | 
	 *  destination_latitude |   destination_elevation |   flight_distance_Nm |   flight_duration_sec
	 *  flight_distance_Nm |   flight_duration_sec
	 */
	
	public void extendWithAirportData( final AirportsDataTable airportsDataTable) {
		
		DoubleColumn origin_latitude_column = DoubleColumn.create("origin_latitude");
		this.flightListDataTable.addColumns(origin_latitude_column);

		DoubleColumn origin_longitude_column = DoubleColumn.create("origin_longitude");
		this.flightListDataTable.addColumns(origin_longitude_column);
		
		FloatColumn origin_elevation_feet_column = FloatColumn.create("origin_elevation_feet");
		this.flightListDataTable.addColumns(origin_elevation_feet_column);
		
		DoubleColumn destination_latitude_column = DoubleColumn.create("destination_latitude");
		this.flightListDataTable.addColumns(destination_latitude_column);
		
		DoubleColumn destination_longitude_column = DoubleColumn.create("destination_longitude");
		this.flightListDataTable.addColumns(destination_longitude_column);
		
		FloatColumn destination_elevation_feet_column = FloatColumn.create("destination_elevation_feet");
		this.flightListDataTable.addColumns(destination_elevation_feet_column);
		
		DoubleColumn flight_distance_Nm_column = DoubleColumn.create("flight_distance_Nm");
		this.flightListDataTable.addColumns(flight_distance_Nm_column);
		
		LongColumn flight_duration_sec_column = LongColumn.create("flight_duration_sec");
		this.flightListDataTable.addColumns(flight_duration_sec_column);
		
		// flight_distance_Nm |   flight_duration_sec
		
		System.out.println( this.flightListDataTable.structure() );
				
		Iterator<Row> iter = this.flightListDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			
			String origin_airport_icao_code = row.getString("origin_icao");
			
			double origin_latitude_degrees = airportsDataTable.getAirportFloatValues(origin_airport_icao_code, "latitude");
			row.setDouble ("origin_latitude" , origin_latitude_degrees);
			
			double origin_longitude_degrees = airportsDataTable.getAirportFloatValues(origin_airport_icao_code, "longitude");
			row.setDouble("origin_longitude" , origin_longitude_degrees);
			
			float origin_elevation_feet = airportsDataTable.getAirportFloatValues(origin_airport_icao_code, "elevation");
			row.setFloat("origin_elevation_feet" , origin_elevation_feet);
			
			String destination_airport_icao_code = row.getString("destination_icao");
			
			double destination_latitude_degrees = airportsDataTable.getAirportFloatValues(destination_airport_icao_code, "latitude");
			row.setDouble("destination_longitude" , destination_latitude_degrees);
			
			double destination_longitude_degrees = airportsDataTable.getAirportFloatValues(destination_airport_icao_code, "longitude");
			row.setDouble("destination_latitude" , destination_longitude_degrees);
			
			float destination_elevation_meters = airportsDataTable.getAirportFloatValues(destination_airport_icao_code, "elevation");
			row.setFloat("destination_elevation_feet" , destination_elevation_meters);
			
			// distance in Nautical Miles
			double Distance_Nm = Utils.calculateHaversineDistanceNauticalMiles(origin_latitude_degrees, origin_longitude_degrees, 
					 destination_latitude_degrees,  destination_longitude_degrees);
			row.setDouble("flight_distance_Nm", Distance_Nm);
			
			Instant takeoff = row.getInstant("takeoff");
			Instant landed = row.getInstant("landed");
			long flight_duration_seconds = Duration.between(takeoff, landed).getSeconds();

			row.setLong ("flight_duration_sec" , flight_duration_seconds);
			
		}
		System.out.println( this.flightListDataTable.print(10));
		
	}
	
	public void extendWithSinusCosinusOfLatitudeLongitude() {
		
		// origin
		
		DoubleColumn origin_latitude_cosine_column = DoubleColumn.create("origin_latitude_cosine");
		this.flightListDataTable.addColumns(origin_latitude_cosine_column);
		
		DoubleColumn origin_latitude_sine_column = DoubleColumn.create("origin_latitude_sine");
		this.flightListDataTable.addColumns(origin_latitude_sine_column);

		DoubleColumn origin_longitude_cosine_column = DoubleColumn.create("origin_longitude_cosine");
		this.flightListDataTable.addColumns(origin_longitude_cosine_column);

		DoubleColumn origin_longitude_sine_column = DoubleColumn.create("origin_longitude_sine");
		this.flightListDataTable.addColumns(origin_longitude_sine_column);
		
		// destination
		
		DoubleColumn destination_latitude_cosine_column = DoubleColumn.create("destination_latitude_cosine");
		this.flightListDataTable.addColumns(destination_latitude_cosine_column);
		
		DoubleColumn destination_latitude_sine_column = DoubleColumn.create("destination_latitude_sine");
		this.flightListDataTable.addColumns(destination_latitude_sine_column);

		DoubleColumn destination_longitude_cosine_column = DoubleColumn.create("destination_longitude_cosine");
		this.flightListDataTable.addColumns(destination_longitude_cosine_column);

		DoubleColumn destination_longitude_sine_column = DoubleColumn.create("destination_longitude_sine");
		this.flightListDataTable.addColumns(destination_longitude_sine_column);
		
		
		System.out.println( this.flightListDataTable.structure() );
		
		Iterator<Row> iter = this.flightListDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			
			double origin_latitude_degrees = row.getDouble("origin_latitude");
			double origin_longitude_degrees = row.getDouble("origin_longitude");
			
			double destination_latitude_degrees = row.getDouble("destination_latitude");
			double destination_longitude_degrees = row.getDouble("destination_longitude");
					
			row.setDouble("origin_latitude_cosine" , Math.cos(Math.toRadians(origin_latitude_degrees)));
			row.setDouble("origin_latitude_sine" , Math.sin(Math.toRadians(origin_latitude_degrees)));
			
			row.setDouble("origin_longitude_cosine" , Math.cos(Math.toRadians(origin_longitude_degrees)));
			row.setDouble("origin_longitude_sine" , Math.sin(Math.toRadians(origin_longitude_degrees)));
			
			row.setDouble("destination_latitude_cosine" , Math.cos(Math.toRadians(destination_latitude_degrees)));
			row.setDouble("destination_latitude_sine" , Math.sin(Math.toRadians(destination_latitude_degrees)));
			
			row.setDouble("destination_longitude_cosine" , Math.cos(Math.toRadians(destination_longitude_degrees)));
			row.setDouble("destination_longitude_sine" , Math.sin(Math.toRadians(destination_longitude_degrees)));
			
		}

		System.out.println( this.flightListDataTable.print(10));
	}
}
