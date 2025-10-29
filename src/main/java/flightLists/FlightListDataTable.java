package flightLists;

import java.util.Iterator;
import java.util.logging.Logger;

import aircrafts.AircraftsData;
import airports.AirportsDataTable;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListDataSchema.FlightListDataRecord;
import fuel.FuelDataTable;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;
import utils.Utils;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class FlightListDataTable extends Table {
	
	private static final Logger logger = Logger.getLogger(FlightListDataTable.class.getName());

	protected train_rank train_rank_value;
	
	public train_rank getTrain_rank_value() {
		return train_rank_value;
	}
	
	public Table flightListDataTable = null;

	public void setFlightListDataTable(Table flightListDataTable) {
		this.flightListDataTable = flightListDataTable;
	}

	public Table getFlightListDataTable() {
		return this.flightListDataTable;
	}

	FlightListDataTable() {
		super("Flight List Data");
	}

	public Instant getTakeoffInstant( final String flight_id ) {
		
		Selection selection = this.flightListDataTable.stringColumn("flight_id").containsString(flight_id);
		Table filteredTable = this.flightListDataTable.where(selection);
		
		Instant takeoff = filteredTable.instantColumn("takeoff").get(0);
		return takeoff;

	}
	
	public void createEmptyFlightListDataTable( ) {
		this.flightListDataTable = Table.create("Flight List Data",

				StringColumn.create("flight_id"),
				// assumption is UTC or Zulu Date ???
				DateColumn.create("flight_date"),

				InstantColumn.create("takeoff"),

				StringColumn.create("origin_icao"),
				StringColumn.create("origin_name"),

				InstantColumn.create("landed"),

				StringColumn.create("destination_icao"),
				StringColumn.create("destination_name"),

				StringColumn.create("aircraft_type")
				
				);
	}
	
	/**
	 * added 26 October 2025
	 */
	public void extendWithFlightDateData( ) {
		
		IntColumn flight_date_year_column = IntColumn.create("flight_date_year");
		this.flightListDataTable.addColumns(flight_date_year_column);

		IntColumn flight_date_month_column = IntColumn.create("flight_date_month");
		this.flightListDataTable.addColumns(flight_date_month_column);

		IntColumn flight_date_day_of_the_year_column = IntColumn.create("flight_date_day_of_the_year");
		this.flightListDataTable.addColumns(flight_date_day_of_the_year_column);

		Iterator<Row> iter = this.flightListDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			
			// assumption date is UTC
			ZonedDateTime zonedFlightDateTime = row.getDate("flight_date").atStartOfDay(ZoneId.of("UTC"));
			int year = zonedFlightDateTime.getYear();
			assert year > 2020 && year < 2026;
			row.setInt( "flight_date_year" , year);
			
			int month = zonedFlightDateTime.getMonth().ordinal()+1;
			assert (month >= 1) && (month <= 12);
			row.setInt( "flight_date_month" , month);
			
			int day_of_the_year = zonedFlightDateTime.getDayOfYear();
			// 366 for the leap year
			assert (day_of_the_year >= 1 ) && (day_of_the_year <= 366 );
			row.setInt( "flight_date_day_of_the_year" , day_of_the_year);
			
		}
	}

	public void appendRowToFlightListDataTable(  FlightListDataRecord record ) {

		Row row = this.flightListDataTable.appendRow();
		
		row.setString("flight_id", record.flight_id());
		row.setDate("flight_date", record.flight_date());
		
		row.setInstant("takeoff", record.takeoff());
		
		row.setString("origin_icao", record.origin_icao());
		row.setString("origin_name", record.origin_name());
		
		row.setInstant("landed", record.landed());

		row.setString("destination_icao", record.destination_icao());
		row.setString("destination_name", record.destination_name());

		row.setString("aircraft_type", record.aircraft_type());
		
	};
	
	
	/*
	 * origin_longitude |   origin_latitude |   origin_elevation |   destination_longitude | 
	 *  destination_latitude |   destination_elevation |   flight_distance_Nm |   flight_duration_sec
	 *  flight_distance_Nm |   flight_duration_sec
	 */
	
	public void extendWithAirportData( final AirportsDataTable airportsDataTable) {
		
		// latitude longitude in degrees - origin airport
		DoubleColumn origin_latitude_deg_column = DoubleColumn.create("origin_latitude_deg");
		this.flightListDataTable.addColumns(origin_latitude_deg_column);

		DoubleColumn origin_longitude_deg_column = DoubleColumn.create("origin_longitude_deg");
		this.flightListDataTable.addColumns(origin_longitude_deg_column);
		
		// latitude and longitude in radians - origin airport
		DoubleColumn origin_latitude_rad_column = DoubleColumn.create("origin_latitude_rad");
		this.flightListDataTable.addColumns(origin_latitude_rad_column);

		DoubleColumn origin_longitude_rad_column = DoubleColumn.create("origin_longitude_rad");
		this.flightListDataTable.addColumns(origin_longitude_rad_column);
		
		// origin airport elevation
		FloatColumn origin_elevation_feet_column = FloatColumn.create("origin_elevation_feet");
		this.flightListDataTable.addColumns(origin_elevation_feet_column);
		
		// destination airport latitude and longitude in degrees
		DoubleColumn destination_latitude_deg_column = DoubleColumn.create("destination_latitude_deg");
		this.flightListDataTable.addColumns(destination_latitude_deg_column);
		
		DoubleColumn destination_longitude_deg_column = DoubleColumn.create("destination_longitude_deg");
		this.flightListDataTable.addColumns(destination_longitude_deg_column);
		
		// destination airport latitude and longitude in radians
		DoubleColumn destination_latitude_rad_column = DoubleColumn.create("destination_latitude_rad");
		this.flightListDataTable.addColumns(destination_latitude_rad_column);
		
		DoubleColumn destination_longitude_rad_column = DoubleColumn.create("destination_longitude_rad");
		this.flightListDataTable.addColumns(destination_longitude_rad_column);
		
		//------------------

		FloatColumn destination_elevation_feet_column = FloatColumn.create("destination_elevation_feet");
		this.flightListDataTable.addColumns(destination_elevation_feet_column);
		
		DoubleColumn flight_distance_Nm_column = DoubleColumn.create("flight_distance_Nm");
		this.flightListDataTable.addColumns(flight_distance_Nm_column);
		
		LongColumn flight_duration_sec_column = LongColumn.create("flight_duration_sec");
		this.flightListDataTable.addColumns(flight_duration_sec_column);
		
		// flight_distance_Nm |   flight_duration_sec
		
		logger.info( this.flightListDataTable.structure().print());
				
		Iterator<Row> iter = this.flightListDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			
			String origin_airport_icao_code = row.getString("origin_icao");
			
			// origin airport latitude and longitude
			double origin_latitude_degrees = airportsDataTable.getAirportDoubleValues(origin_airport_icao_code, "latitude");
			row.setDouble ("origin_latitude_deg" , origin_latitude_degrees);
			
			double origin_longitude_degrees = airportsDataTable.getAirportDoubleValues(origin_airport_icao_code, "longitude");
			row.setDouble("origin_longitude_deg" , origin_longitude_degrees);
			
			// -------------- destination airport latitude longitude radians (27/10/2025)
			row.setDouble("origin_longitude_rad" , ( origin_latitude_degrees * Math.PI ) / 180.0);
			row.setDouble("origin_latitude_rad" , ( origin_longitude_degrees * Math.PI ) / 180.0);

			// elevation origin airport
			float origin_elevation_feet = airportsDataTable.getAirportFloatValues(origin_airport_icao_code, "elevation");
			row.setFloat("origin_elevation_feet" , origin_elevation_feet);
			
			// icao code
			String destination_airport_icao_code = row.getString("destination_icao");
			
			// destination airport latitude longitude degrees
			double destination_latitude_degrees = airportsDataTable.getAirportDoubleValues(destination_airport_icao_code, "latitude");
			row.setDouble("destination_longitude_deg" , destination_latitude_degrees);
			
			double destination_longitude_degrees = airportsDataTable.getAirportDoubleValues(destination_airport_icao_code, "longitude");
			row.setDouble("destination_latitude_deg" , destination_longitude_degrees);
			
			// -------------- destination airport latitude longitude radians (27/10/2025)
			row.setDouble("destination_longitude_rad" , ( destination_latitude_degrees * Math.PI ) / 180.0);
			row.setDouble("destination_latitude_rad" , ( destination_longitude_degrees * Math.PI ) / 180.0);

			// elevation of the destination airport
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
		logger.info( this.flightListDataTable.structure().print());
		logger.info( this.flightListDataTable.print());
	}
	
	@SuppressWarnings("deprecation")
	public void extendWithAircraftsData( AircraftsData aircraftsData ) {
		
		// use left join
		// aircraft_type -> join column in FlightList table
		// ICAO_Code -> join column in aircrafts table
		logger.info(aircraftsData.getAircraftDataTable().shape());
		logger.info(aircraftsData.getAircraftDataTable().structure().print());

		// Rename the column
		aircraftsData.getAircraftDataTable().column("ICAO_Code").setName("aircraft_type");
		logger.info(aircraftsData.getAircraftDataTable().structure().print());
		
		// Perform an left outer join on the "id" column
		// Left Outer Join: Keeps all rows from the left table and matches from the right.
		Table aircraftsDataTable = aircraftsData.getAircraftDataTable();
        this.setFlightListDataTable( this.flightListDataTable.joinOn("aircraft_type").leftOuter(aircraftsDataTable));
		
		logger.info( this.flightListDataTable.structure().print());
		logger.info( this.flightListDataTable.print(10));

	}
	
	public void extendWithAirportsSinusCosinusOfLatitudeLongitude() {
		
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
