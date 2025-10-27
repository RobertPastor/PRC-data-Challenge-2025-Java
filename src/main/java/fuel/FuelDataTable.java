package fuel;


import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.logging.Logger;

import aircrafts.AircraftsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;
import flightLists.FlightListDataTable;
import flights.FlightData;
import flights.FlightDataTable;
import fuel.FuelDataSchema.FuelDataRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import utils.Utils;

public class FuelDataTable extends Table {
	
	private static final Logger logger = Logger.getLogger(FuelDataTable.class.getName());

	protected train_rank train_rank_value;

	public void setTrain_rank_value(train_rank train_rank_value) {
		this.train_rank_value = train_rank_value;
	}

	public train_rank getTrain_rank_value() {
		return train_rank_value;
	}
 
	public Table fuelDataTable = null;

	public void setFuelDataTable(Table fuelDataTable) {
		this.fuelDataTable = fuelDataTable;
	}

	public Table getFuelDataTable() {
		return this.fuelDataTable;
	}

	protected FuelDataTable(train_rank train_rank_value) {
		super("Fuel Data");
		this.setTrain_rank_value(train_rank_value);
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
		
		System.out.println ( flightListDataTable.structure() );
		System.out.println ( flightListDataTable.shape() );
		System.out.println ( flightListDataTable.print(10) );
		
		// apply join using  flight_id
		// use left outer join from fuel table to flight list table
		// from flight list data use column ("flight_id")
		
		// Perform an left outer join on the "id" column
		// Left Outer Join: Keeps all rows from the left table and matches from the right.
		this.setFuelDataTable( this.fuelDataTable.joinOn("flight_id").leftOuter(flightListDataTable));
		System.out.println( this.getFuelDataTable().structure() );
		
		
	}
	
	/**
	 * used during reading of the input parquet rows , hence records
	 * @param record
	 */
	public void appendRowToFuelDataTable( final FuelDataRecord record ) {

		Row row = this.fuelDataTable.appendRow();
		
		row.setInt ("idx", record.idx());
		row.setString("flight_id", record.flight_id());
		
		Instant start = record.start();
		row.setInstant("start", start);
		
		Instant end = record.end();
		row.setInstant("end", end);
		
		//assertion start is before end
		assert start.isBefore(end);
		
		row.setFloat ("fuel_kg", record.fuel_kg());
	}
	
	public void extendFuelWithEndStartDifference( ) {
		
		LongColumn time_diff_seconds_column = LongColumn.create("time_diff_seconds");
		this.fuelDataTable.addColumns(time_diff_seconds_column);
		
		System.out.println( this.fuelDataTable.structure() );
		//int maxRowCount = this.fuelDataTable.rowCount();
		
		Iterator<Row> iter = this.fuelDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			
			Instant start = row.getInstant("start");
			Instant end = row.getInstant("end");
			
			long difference = Duration.between(start, end).toSeconds();
			row.setLong("time_diff_seconds" , difference);
		}
		//System.out.println( this.fuelDataTable.print(10) );
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
			// warning - assertion to clean the data
			assert start.isBefore(end);
			
			long difference = Duration.between(start, end).toSeconds();

			float fuel_flow_kg_seconds = (float) 0.0;
			if ( difference > 0.0 ) {
				fuel_flow_kg_seconds = fuel_kg / difference;
			}
			row.setFloat("fuel_flow_kg_sec" , fuel_flow_kg_seconds);
		}
		System.out.println( this.fuelDataTable.print(10));
	}
	
	public void extendFuelStartEndInstantsWithFlightData( final int maxComputedRow ) throws IOException {
		
		DoubleColumn aircraft_latitude_at_fuel_start_column = DoubleColumn.create("aircraft_latitude_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_latitude_at_fuel_start_column);

		DoubleColumn aircraft_longitude_at_fuel_start_column = DoubleColumn.create("aircraft_longitude_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_longitude_at_fuel_start_column);
		
		// latitude
		DoubleColumn aircraft_latitude_at_fuel_end_column = DoubleColumn.create("aircraft_latitude_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_latitude_at_fuel_end_column);
		DoubleColumn aircraft_longitude_at_fuel_end_column = DoubleColumn.create("aircraft_longitude_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_longitude_at_fuel_end_column);
		
		// compute distance flown from start to end
		DoubleColumn aircraft_distance_flown_column = DoubleColumn.create("aircraft_distance_flown_Nm");
		this.fuelDataTable.addColumns(aircraft_distance_flown_column);
		
		// ------------- altitude
		FloatColumn aircraft_altitude_start_column = FloatColumn.create("aircraft_altitude_ft_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_altitude_start_column);
		FloatColumn aircraft_altitude_end_column = FloatColumn.create("aircraft_altitude_ft_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_altitude_end_column);
		
		// computed vertical rate feet per minutes
		FloatColumn aircraft_computed_vertical_rate = FloatColumn.create("aircraft_computed_vertical_rate_ft_min");
		this.fuelDataTable.addColumns(aircraft_computed_vertical_rate);
		
		// ground speed
		FloatColumn aircraft_groundspeed_start_column = FloatColumn.create("aircraft_groundspeed_kt_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_groundspeed_start_column);
		FloatColumn aircraft_groundspeed_end_column = FloatColumn.create("aircraft_groundspeed_kt_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_groundspeed_end_column);

		FloatColumn aircraft_track_angle_start_column = FloatColumn.create("aircraft_track_angle_deg_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_track_angle_start_column);
		FloatColumn aircraft_track_angle_end_column = FloatColumn.create("aircraft_track_angle_deg_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_track_angle_end_column);

		FloatColumn aircraft_vertical_rate_start_column = FloatColumn.create("aircraft_vertical_rate_ft_min_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_vertical_rate_start_column);
		
		FloatColumn aircraft_vertical_rate_end_column = FloatColumn.create("aircraft_vertical_rate_ft_min_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_vertical_rate_end_column);
		
		// find the nearest instant from a fuel table of a flight id
		// given a fuel start or stop instant
		train_rank train_rank_value = this.getTrain_rank_value();
		
		Iterator<Row> iter = this.fuelDataTable.iterator();
		int counter = 0;
		while ( iter.hasNext() && ( counter < maxComputedRow )) {
			counter++;
			Row row = iter.next();
			
			Instant start = row.getInstant("start");
			Instant end = row.getInstant("end");
			
			String flight_id = row.getString ("flight_id");
			FlightData flightData = new FlightData( train_rank_value , flight_id );
			flightData.readParquet();
			
			System.out.println("--------------------------------------");
			System.out.println("----------------- row count = "+ counter + " / max = " + this.fuelDataTable.rowCount() + " ---------------------");
			System.out.println("--------------------------------------");
			
			//Instant flightNearestInstantFromFuelStart = flightData.findNearestIntantFromFlightTimeStamps (start);
			//Instant flightNearestInstantFromFuelEnd = flightData.findNearestIntantFromFlightTimeStamps (end);
			
			//logger.info("nearest from fuel start = " + flightNearestInstantFromFuelStart);
			//logger.info("nearest from fuel end = " + flightNearestInstantFromFuelEnd);
			
			double ac_lat_fuel_start = flightData.getDoubleFlightDataAtNearestFuelInstant("latitude" , "start" , start);
			double ac_lon_fuel_start = flightData.getDoubleFlightDataAtNearestFuelInstant("longitude", "start" ,start);
			
			double ac_lat_fuel_end = flightData.getDoubleFlightDataAtNearestFuelInstant("latitude" , "end" ,end);
			double ac_lon_fuel_end = flightData.getDoubleFlightDataAtNearestFuelInstant("longitude" , "end" ,end);
			
			row.setDouble("aircraft_latitude_at_fuel_start" , ac_lat_fuel_start);
			row.setDouble("aircraft_longitude_at_fuel_start" , ac_lon_fuel_start);
			
			row.setDouble("aircraft_latitude_at_fuel_end" , ac_lat_fuel_end);
			row.setDouble("aircraft_longitude_at_fuel_end" , ac_lon_fuel_end);
			
			// compute distance flown in Nautical miles between fuel start and fuel end
			double distanceFlownBetweenStartEnd = Utils.calculateHaversineDistanceNauticalMiles( ac_lat_fuel_start, ac_lon_fuel_start, ac_lat_fuel_end, ac_lon_fuel_end); 
			row.setDouble("aircraft_distance_flown_Nm" , distanceFlownBetweenStartEnd);
					
			float altitude_start = flightData.getFloatFlightDataAtNearestFuelInstant("altitude" , "start" , start);
			row.setFloat("aircraft_altitude_ft_at_fuel_start" , altitude_start);
			
			float altitude_end = flightData.getFloatFlightDataAtNearestFuelInstant("altitude" , "end" , end);
			row.setFloat("aircraft_altitude_ft_at_fuel_end" , altitude_end);
			
			// computed vertical rate feet per minutes
			long time_diff_sec = row.getLong("time_diff_seconds");
			float computed_vertical_rate = (altitude_end - altitude_start)/ (float)time_diff_sec;
			row.setFloat("aircraft_computed_vertical_rate_ft_min" , computed_vertical_rate);

			float groundSpeed_start = flightData.getFloatFlightDataAtNearestFuelInstant("groundspeed", "start" , start);
			row.setFloat("aircraft_groundspeed_kt_at_fuel_start" , groundSpeed_start);
			
			float groundSpeed_end = flightData.getFloatFlightDataAtNearestFuelInstant("groundspeed" , "end" , end);
			row.setFloat("aircraft_groundspeed_kt_at_fuel_end" , groundSpeed_end);
			
			float track_angle_deg_start = flightData.getFloatFlightDataAtNearestFuelInstant("track" , "start" , start);
			row.setFloat("aircraft_track_angle_deg_at_fuel_start" , track_angle_deg_start);
			
			float track_angle_deg_end =flightData.getFloatFlightDataAtNearestFuelInstant("track" , "end", end);
			row.setFloat("aircraft_track_angle_deg_at_fuel_end" ,  track_angle_deg_end);
			
			float vertical_rate_ft_min_start = flightData.getFloatFlightDataAtNearestFuelInstant("vertical_rate" , "start" , start);
			row.setFloat("aircraft_vertical_rate_ft_min_at_fuel_start" , vertical_rate_ft_min_start);
			
			float vertical_rate_ft_min_end = flightData.getFloatFlightDataAtNearestFuelInstant("vertical_rate" , "end" , end);
			row.setFloat("aircraft_vertical_rate_ft_min_at_fuel_end" , vertical_rate_ft_min_end);
		}
	}
	
	public void extendRelativeStartEndFromFlightTakeoff() {

		LongColumn fuel_burnt_start_relative_to_takeoff_sec_column = LongColumn.create("fuel_burnt_start_relative_to_takeoff_sec");
		this.fuelDataTable.addColumns(fuel_burnt_start_relative_to_takeoff_sec_column);
		
		LongColumn fuel_burnt_end_relative_to_takeoff_sec_column = LongColumn.create("fuel_burnt_end_relative_to_takeoff_sec");
		this.fuelDataTable.addColumns(fuel_burnt_end_relative_to_takeoff_sec_column);
		
		LongColumn fuel_burnt_end_relative_to_landed_sec_column = LongColumn.create("fuel_burnt_end_relative_to_landed_sec");
		this.fuelDataTable.addColumns(fuel_burnt_end_relative_to_landed_sec_column );

		Iterator<Row> iter = this.fuelDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			
			Instant start = row.getInstant("start");
			Instant end = row.getInstant("end");
			assert start.isBefore(end);
			
			Instant takeoff = row.getInstant("takeoff");
			Instant landed = row.getInstant("landed");
			
			if ( ( takeoff != null ) && takeoff.isBefore(landed) ) {
				assert takeoff.isBefore(landed);
			}	else {
				System.out.println(row.getRowNumber());
			}
			
			if ( ( takeoff != null ) && takeoff.isBefore(start) ) {
				assert takeoff.isBefore(start);
				long duration_sec = Duration.between(takeoff, start).toSeconds();
				row.setLong("fuel_burnt_start_relative_to_takeoff_sec" , duration_sec);
			} else {
				System.out.println("Error takeoff = " + takeoff + " -- not before fuel burnt start = " + start);
			}
			
			if ( ( takeoff != null ) && takeoff.isBefore(end) ) {
				assert takeoff.isBefore(end);
				long duration_sec = Duration.between(takeoff, end).toSeconds();
				row.setLong("fuel_burnt_end_relative_to_takeoff_sec" , duration_sec);
			} else {
				System.out.println("Error takeoff = " + takeoff + " -- not before fuel burnt end = " + end);
			}
			
			if ( ( landed != null ) && ( landed.getEpochSecond() > 0 )) {
				if ( end.isBefore(landed) ) {
					long duration_sec = Duration.between(end, landed).toSeconds();
					row.setLong("fuel_burnt_end_relative_to_landed_sec" , duration_sec);
				}
			} else {
				System.out.println("Error fuel burnt end = " + end + " -- not before landed = " + landed);
			}
		}
	}
	
}

