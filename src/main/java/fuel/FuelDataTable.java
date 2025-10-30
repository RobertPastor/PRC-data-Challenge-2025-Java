package fuel;


import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import aircrafts.AircraftsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;
import flightLists.FlightListDataTable;
import flights.FlightData;
import flights.FlightDataInterpolation;
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
	
	private Map<Integer, ArrayList<Instant>> errorsMap = null;

	public Map<Integer, ArrayList<Instant>> getErrorsMap() {
		return errorsMap;
	}

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
	
	private FlightDataInterpolation flightDataInterpolation;

	// constructor
	protected FuelDataTable(train_rank train_rank_value) {
		super("Fuel Data");
		this.setTrain_rank_value(train_rank_value);
		logger.info("constructor");
		
		errorsMap = new HashMap<Integer,ArrayList<Instant>>();
		// prepare for interpolation functions
		
		final List<String> flightDatacolumnsToInterpolateList = 
				Arrays.asList("latitude" , "longitude", "altitude",
				"groundspeed","track", "vertical_rate", "mach", "TAS", "CAS");
		
		this.flightDataInterpolation = new FlightDataInterpolation(flightDatacolumnsToInterpolateList);
	}
	
	protected void generateListOfErrors() {
		
		// Iterate through the map
		for (Map.Entry<Integer, ArrayList<Instant>> entry : errorsMap.entrySet()) {
			System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
		}
		
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
		
		logger.info ( flightListDataTable.structure().print() );
		logger.info ( flightListDataTable.shape() );
		logger.info( flightListDataTable.print(10) );
		
		// apply join using  flight_id
		// use left outer join from fuel table to flight list table
		// from flight list data use column ("flight_id")
		
		// Perform an left outer join on the "id" column
		// Left Outer Join: Keeps all rows from the left table and matches from the right.
		this.setFuelDataTable( this.fuelDataTable.joinOn("flight_id").leftOuter(flightListDataTable));

		logger.info( this.getFuelDataTable().shape() );
		logger.info( this.getFuelDataTable().structure().print() );
		
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
		
		logger.info( this.fuelDataTable.structure().print() );
		
		Iterator<Row> iter = this.fuelDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			
			int idx = row.getInt("idx");
			Float fuel_kg = row.getFloat ("fuel_kg");
			
			Instant start = row.getInstant("start");
			Instant end = row.getInstant("end");
			
			// warning - assertion to clean the data
			assert start.isBefore(end);
			
			long instantDifferenceSeconds = Duration.between(start, end).toSeconds();
			// 29 October 2025 - still to demonstrate that fuel flow is a better Y than fuel Kg only
			float fuel_flow_kg_seconds = (float) 0.0;
			if ( instantDifferenceSeconds > 0.0 ) {
				fuel_flow_kg_seconds = fuel_kg / Math.abs(instantDifferenceSeconds);
			} else {
				errorsMap.put(idx, new ArrayList<>(List.of(start,end)));
				fuel_flow_kg_seconds = fuel_kg / Math.abs(instantDifferenceSeconds);
			}
			row.setFloat("fuel_flow_kg_sec" , fuel_flow_kg_seconds);
		}
		logger.info( this.fuelDataTable.print(10));
	}
	
	public void extendFuelStartEndInstantsWithFlightData( final long maxToBeComputedRow ) throws IOException {
		
		// latitude degrees at fuel start
		DoubleColumn aircraft_latitude_deg_at_fuel_start_column = DoubleColumn.create("aircraft_latitude_deg_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_latitude_deg_at_fuel_start_column);
		
		// latitude radians at fuel start
		DoubleColumn aircraft_latitude_rad_at_fuel_start_column = DoubleColumn.create("aircraft_latitude_rad_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_latitude_rad_at_fuel_start_column);
		
		// longitude degrees at fuel start
		DoubleColumn aircraft_longitude_deg_at_fuel_start_column = DoubleColumn.create("aircraft_longitude_deg_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_longitude_deg_at_fuel_start_column);
		
		// longitude radians at fuel start
		DoubleColumn aircraft_longitude_rad_at_fuel_start_column = DoubleColumn.create("aircraft_longitude_rad_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_longitude_rad_at_fuel_start_column);
		
		// latitude degrees at fuel end
		DoubleColumn aircraft_latitude_deg_at_fuel_end_column = DoubleColumn.create("aircraft_latitude_deg_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_latitude_deg_at_fuel_end_column);
		
		// latitude radians at fuel end
		DoubleColumn aircraft_latitude_rad_at_fuel_end_column = DoubleColumn.create("aircraft_latitude_rad_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_latitude_rad_at_fuel_end_column);

		// longitude degrees at fuel end
		DoubleColumn aircraft_longitude_deg_at_fuel_end_column = DoubleColumn.create("aircraft_longitude_deg_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_longitude_deg_at_fuel_end_column);
		
		// longitude radians at fuel end
		DoubleColumn aircraft_longitude_rad_at_fuel_end_column = DoubleColumn.create("aircraft_longitude_rad_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_longitude_rad_at_fuel_end_column);
		
		// compute distance flown from start to end
		DoubleColumn aircraft_distance_flown_column = DoubleColumn.create("aircraft_distance_flown_Nm");
		this.fuelDataTable.addColumns(aircraft_distance_flown_column);
		
		// ------------- altitude
		DoubleColumn aircraft_altitude_start_column = DoubleColumn.create("aircraft_altitude_ft_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_altitude_start_column);
		
		DoubleColumn aircraft_altitude_end_column = DoubleColumn.create("aircraft_altitude_ft_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_altitude_end_column);
		
		// computed vertical rate feet per minutes
		DoubleColumn aircraft_computed_vertical_rate = DoubleColumn.create("aircraft_computed_vertical_rate_ft_min");
		this.fuelDataTable.addColumns(aircraft_computed_vertical_rate);
		
		// ground speed in knots
		DoubleColumn aircraft_groundspeed_start_column = DoubleColumn.create("aircraft_groundspeed_kt_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_groundspeed_start_column);
		
		DoubleColumn aircraft_groundspeed_end_column = DoubleColumn.create("aircraft_groundspeed_kt_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_groundspeed_end_column);
		
		// ground speed X and Y in knots and fuel start
		DoubleColumn aircraft_groundspeed_X_start_column = DoubleColumn.create("aircraft_groundspeed_kt_X_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_groundspeed_X_start_column);
		
		DoubleColumn aircraft_groundspeed_X_end_column = DoubleColumn.create("aircraft_groundspeed_kt_X_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_groundspeed_X_end_column);

		// ground speed X and Y in knots and fuel start
		DoubleColumn aircraft_groundspeed_Y_start_column = DoubleColumn.create("aircraft_groundspeed_kt_Y_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_groundspeed_Y_start_column);
		
		DoubleColumn aircraft_groundspeed_Y_end_column = DoubleColumn.create("aircraft_groundspeed_kt_Y_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_groundspeed_Y_end_column);
		
		// track angle degrees fuel start
		DoubleColumn aircraft_track_angle_deg_start_column = DoubleColumn.create("aircraft_track_angle_deg_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_track_angle_deg_start_column);
		
		// track angle degrees fuel end
		DoubleColumn aircraft_track_angle_deg_end_column = DoubleColumn.create("aircraft_track_angle_deg_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_track_angle_deg_end_column);
		
		// track angle radians
		DoubleColumn aircraft_track_angle_rad_start_column = DoubleColumn.create("aircraft_track_angle_rad_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_track_angle_rad_start_column);
		
		DoubleColumn aircraft_track_angle_rad_end_column = DoubleColumn.create("aircraft_track_angle_rad_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_track_angle_rad_end_column);

		// vertical rate
		DoubleColumn aircraft_vertical_rate_start_column = DoubleColumn.create("aircraft_vertical_rate_ft_min_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_vertical_rate_start_column);
		
		DoubleColumn aircraft_vertical_rate_end_column = DoubleColumn.create("aircraft_vertical_rate_ft_min_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_vertical_rate_end_column);
		
		// mach at start and end
		DoubleColumn aircraft_mach_start_column = DoubleColumn.create("aircraft_mach_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_mach_start_column);
		
		DoubleColumn aircraft_mach_end_column = DoubleColumn.create("aircraft_mach_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_mach_end_column);
		
		// TAS
		DoubleColumn aircraft_TAS_start_column = DoubleColumn.create("aircraft_TAS_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_TAS_start_column);
		
		DoubleColumn aircraft_TAS_end_column = DoubleColumn.create("aircraft_TAS_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_TAS_end_column);
		
		// CAS
		DoubleColumn aircraft_CAS_start_column = DoubleColumn.create("aircraft_CAS_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_CAS_start_column);
		
		DoubleColumn aircraft_CAS_end_column = DoubleColumn.create("aircraft_CAS_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_CAS_end_column);

		// find the nearest instant from a fuel table of a flight id
		// given a fuel start or stop instant
		train_rank train_rank_value = this.getTrain_rank_value();
		
		Iterator<Row> iter = this.fuelDataTable.iterator();
		long counter = 0;
		while ( iter.hasNext() && ( counter < maxToBeComputedRow )) {
			counter++;
			Row row = iter.next();
			
			Instant start = row.getInstant("start");
			Instant end = row.getInstant("end");
			
			String flight_id = row.getString ("flight_id");
			FlightData flightData = new FlightData( train_rank_value , flight_id );
			// 27th October 2025 - use new stream reader capable of filling empty values
			// read one flight data parquet file
			flightData.readParquetWithStream();
			
			logger.info(flightData.getFlightDataTable().shape() );
			logger.info(flightData.getFlightDataTable().structure().print() );
			
			// one set of interpolation function for each loaded flight data frame
			this.flightDataInterpolation.buildInterpolationFunctions(flightData.getFlightDataTable());
			
			logger.info("--------------------------------------");
			logger.info("----------------- row count = "+ counter + " / max = " + this.fuelDataTable.rowCount() + " ---------------------");
			logger.info("--------------------------------------");
			
			double ac_lat_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("latitude" , start);
			double ac_lon_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("longitude" ,start);
			
			double ac_lat_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("latitude"  ,end);
			double ac_lon_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("longitude"  ,end);
			
			row.setDouble("aircraft_latitude_deg_at_fuel_start" , ac_lat_fuel_start);
			row.setDouble("aircraft_latitude_rad_at_fuel_start" , (ac_lat_fuel_start * Math.PI)/180.0);
			
			row.setDouble("aircraft_longitude_deg_at_fuel_start" , ac_lon_fuel_start);
			row.setDouble("aircraft_longitude_rad_at_fuel_start" , ( ac_lon_fuel_start * Math.PI)/180.0);
			
			row.setDouble("aircraft_latitude_deg_at_fuel_end" , ac_lat_fuel_end);
			row.setDouble("aircraft_latitude_rad_at_fuel_end" , ( ac_lat_fuel_end * Math.PI)/180.0);
			
			row.setDouble("aircraft_longitude_deg_at_fuel_end" , ac_lon_fuel_end);
			row.setDouble("aircraft_longitude_rad_at_fuel_end" , (ac_lon_fuel_end * Math.PI)/180.0);
			
			// compute distance flown in Nautical miles between fuel start and fuel end
			double distanceFlownBetweenStartEnd = Utils.calculateHaversineDistanceNauticalMiles( ac_lat_fuel_start, ac_lon_fuel_start, ac_lat_fuel_end, ac_lon_fuel_end); 
			row.setDouble("aircraft_distance_flown_Nm" , distanceFlownBetweenStartEnd);
					
			double altitude_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  start);
			row.setDouble("aircraft_altitude_ft_at_fuel_start" , altitude_start);
			
			double altitude_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  end);
			row.setDouble("aircraft_altitude_ft_at_fuel_end" , altitude_end);
			
			// computed vertical rate feet per minutes
			long time_diff_sec = row.getLong("time_diff_seconds");
			
			double computed_vertical_rate = (altitude_end - altitude_start)/ (float)time_diff_sec;
			row.setDouble("aircraft_computed_vertical_rate_ft_min" , computed_vertical_rate);

			// ground speed
			double groundSpeed_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("groundspeed",  start);
			row.setDouble("aircraft_groundspeed_kt_at_fuel_start" , groundSpeed_start);
			double groundSpeed_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("groundspeed" ,  end);
			row.setDouble("aircraft_groundspeed_kt_at_fuel_end" , groundSpeed_end);
			
			// track angle degrees
			double track_angle_deg_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("track" ,  start);
			row.setDouble("aircraft_track_angle_deg_at_fuel_start" , track_angle_deg_start);
			
			double track_angle_deg_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("track" ,  end);
			row.setDouble("aircraft_track_angle_deg_at_fuel_end" ,  track_angle_deg_end);
			
			// ground speed X and Y projected components
			row.setDouble("aircraft_groundspeed_kt_X_at_fuel_start" , groundSpeed_end * Math.cos(Math.toRadians(track_angle_deg_end)));
			row.setDouble("aircraft_groundspeed_kt_Y_at_fuel_start" , groundSpeed_end * Math.sin(Math.toRadians(track_angle_deg_end)));

			// ground speed X and Y projected components
			row.setDouble("aircraft_groundspeed_kt_X_at_fuel_end" , groundSpeed_start * Math.cos(Math.toRadians(track_angle_deg_start)));
			row.setDouble("aircraft_groundspeed_kt_Y_at_fuel_end" , groundSpeed_start * Math.sin(Math.toRadians(track_angle_deg_start)));

			
			// track angle radians
			row.setDouble("aircraft_track_angle_rad_at_fuel_start" , Math.toRadians(track_angle_deg_start) );
			row.setDouble("aircraft_track_angle_rad_at_fuel_end" ,  Math.toRadians(track_angle_deg_end));
			
			// vertical rate
			double vertical_rate_ft_min_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("vertical_rate" ,  start);
			row.setDouble("aircraft_vertical_rate_ft_min_at_fuel_start" , vertical_rate_ft_min_start);
			
			double vertical_rate_ft_min_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("vertical_rate" ,  end);
			row.setDouble("aircraft_vertical_rate_ft_min_at_fuel_end" , vertical_rate_ft_min_end);
			
			// mach
			double mach_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("mach" ,  start);
			row.setDouble("aircraft_mach_at_fuel_start" , mach_start);
			
			double mach_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("mach" ,  end);
			row.setDouble("aircraft_mach_at_fuel_end" , mach_end);
			
			// TAS
			double TAS_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("TAS" ,  start);
			row.setDouble("aircraft_TAS_at_fuel_start" , TAS_start);
			
			double TAS_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("TAS" ,  end);
			row.setDouble("aircraft_TAS_at_fuel_end" , TAS_end);
			
			// CAS
			double CAS_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("CAS" ,  start);
			row.setDouble("aircraft_CAS_at_fuel_start" , CAS_start);
			
			double CAS_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("CAS" ,  end);
			row.setDouble("aircraft_CAS_at_fuel_end" , CAS_end);
		
		}
	}
	
	/**
	 * 
	 * check that take off is before fuel start instant
	 * check that take off is before fuel end instant
	 * 
	 * compute difference in seconds between take off and fuel start instant
	 * compute difference in seconds between take off and fuel end instant
	 * 
	 * compute difference between fuel end instant and flight landed instant
	 * check that fuel end instant is before flight landed instant
	 * 
	 */
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
			
			int idx = row.getInt("idx");
			
			Instant start = row.getInstant("start");
			Instant end = row.getInstant("end");
			assert start.isBefore(end);
			
			Instant takeoff = row.getInstant("takeoff");
			Instant landed = row.getInstant("landed");
			
			if ( ( takeoff != null ) && takeoff.isBefore(landed) ) {
				assert takeoff.isBefore(landed);
			}	else {
				this.errorsMap.put(idx, new ArrayList<>(List.of(takeoff,landed)));
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

