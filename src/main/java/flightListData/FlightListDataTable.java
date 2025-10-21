package flightListData;

import java.util.Iterator;

import airports.AirportsDataTable;
import flightListData.FlightListDataSchema.FlightListDataRecord;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

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
	 */
	
	public void extendWithAirportData( final AirportsDataTable airportsDataTable) {
		
		FloatColumn origin_latitude_column = FloatColumn.create("origin_latitude");
		this.flightListDataTable.addColumns(origin_latitude_column);

		FloatColumn origin_longitude_column = FloatColumn.create("origin_longitude");
		this.flightListDataTable.addColumns(origin_longitude_column);
		
		FloatColumn origin_elevation_column = FloatColumn.create("origin_elevation");
		this.flightListDataTable.addColumns(origin_elevation_column);
		
		
		FloatColumn destination_latitude_column = FloatColumn.create("destination_latitude");
		this.flightListDataTable.addColumns(destination_latitude_column);
		
		FloatColumn destination_longitude_column = FloatColumn.create("destination_longitude");
		this.flightListDataTable.addColumns(destination_longitude_column);
		
		FloatColumn destination_elevation_column = FloatColumn.create("destination_elevation");
		this.flightListDataTable.addColumns(destination_elevation_column);
		
		System.out.println( this.flightListDataTable.structure() );
				
		Iterator<Row> iter = this.flightListDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			String origin_airport_icao_code = row.getString("origin_icao");
			
			float origin_latitude = airportsDataTable.getAirportFloatValues(origin_airport_icao_code, "latitude");
			row.setFloat("origin_latitude" , origin_latitude);
			
			float origin_longitude = airportsDataTable.getAirportFloatValues(origin_airport_icao_code, "longitude");
			row.setFloat("origin_longitude" , origin_longitude);
			
			float origin_elevation = airportsDataTable.getAirportFloatValues(origin_airport_icao_code, "elevation");
			row.setFloat("origin_elevation" , origin_elevation);
			
			String destination_airport_icao_code = row.getString("destination_icao");
			
			float destination_latitude = airportsDataTable.getAirportFloatValues(destination_airport_icao_code, "latitude");
			row.setFloat("destination_longitude" , destination_latitude);
			
			float destination_longitude = airportsDataTable.getAirportFloatValues(destination_airport_icao_code, "longitude");
			row.setFloat("destination_latitude" , destination_longitude);
			
			float destination_elevation = airportsDataTable.getAirportFloatValues(destination_airport_icao_code, "elevation");
			row.setFloat("destination_elevation" , destination_elevation);
			
		}
		System.out.println( this.flightListDataTable.print(10));
		
	}
}
