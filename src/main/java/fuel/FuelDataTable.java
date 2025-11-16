package fuel;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import atmosphere.AirSpeedConverter;
import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flights.FlightData;
import flights.FlightDataInterpolation;
import folderDiscovery.FolderDiscovery;
import fuel.FuelDataSchema.FuelDataRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import utils.Utils;



public class FuelDataTable extends Table implements Runnable {

	class CustomException extends Exception {
		/**
		 * serial generated ID
		 */
		private static final long serialVersionUID = -1346311432020834637L;
		public CustomException(String message) {
			super(message);
		}
	}

	private static final Logger logger = Logger.getLogger(FuelDataTable.class.getName());

	protected train_rank_final train_rank_value;
	private  long maxToBeComputedRow = 0;
	private Map<Integer, ArrayList<String>> errorsMap = null;

	public Table fuelDataTable = null;
	private AirSpeedConverter airSpeedConverter = null;

	private FlightDataInterpolation flightDataInterpolation;

	// constructor
	protected FuelDataTable(train_rank_final train_rank_value, final long maxToBeComputedRow) {
		super("Fuel Data");
		this.setTrain_rank_value(train_rank_value);
		this.setMaxToBeComputedRow(maxToBeComputedRow);
		logger.info("constructor");

		errorsMap = new HashMap<Integer,ArrayList<String>>();
		// prepare for interpolation functions

		final List<String> flightDatacolumnsToInterpolateList = 
				Arrays.asList("latitude" , "longitude", "altitude",
						"groundspeed","track", "vertical_rate", "mach", "TAS", "CAS");

		this.flightDataInterpolation = new FlightDataInterpolation(flightDatacolumnsToInterpolateList);
		this.airSpeedConverter = new AirSpeedConverter();

	}

	public void generateListOfErrors() {

		String currentDateTimeAsStr = Utils.getCurrentDateTimeasStr();
		String fileName  = "Errors_" + this.getTrain_rank_value() + "_" + currentDateTimeAsStr + ".txt";

		String folderStr = FolderDiscovery.getTrainRankOutputfolderStr();

		Path path = Paths.get(folderStr , fileName );
		File file = path.toFile();

		try (FileWriter writer = new FileWriter(file.getAbsoluteFile())) {
			// write the header
			writer.write( "Error" + ";" + "Value");
			// Iterate through the map
			for (Map.Entry<Integer, ArrayList<String>> entry : errorsMap.entrySet()) {
				System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
				writer.write( entry.getKey() + ";" + entry.getValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
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

		//logger.info ( flightListDataTable.structure().print() );
		//logger.info ( flightListDataTable.shape() );
		//logger.info( flightListDataTable.print(10) );

		// apply join using  flight_id
		// use left outer join from fuel table to flight list table
		// from flight list data use column ("flight_id")

		// Perform an left outer join on the "id" column
		// Left Outer Join: Keeps all rows from the left table and matches from the right.
		this.setFuelDataTable( this.fuelDataTable.joinOn("flight_id").leftOuter(flightListDataTable));

		//logger.info( this.getFuelDataTable().shape() );
		//logger.info( this.getFuelDataTable().structure().print() );
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

		//System.out.println( this.fuelDataTable.structure() );
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
	 * 3rd November 2025
	 * Fuel Flow kg per seconds is the main Y to estimate (not the fuel in kg)
	 */
	public void extendFuelFlowKgSeconds() {

		FloatColumn fuel_flow_kg_sec_column = FloatColumn.create("fuel_flow_kg_sec");
		this.fuelDataTable.addColumns(fuel_flow_kg_sec_column);

		//logger.info( this.fuelDataTable.structure().print() );

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
				errorsMap.put(idx, new ArrayList<>(List.of(row.getString("flight_id"),start.toString(),end.toString())));
				fuel_flow_kg_seconds = fuel_kg / Math.abs(instantDifferenceSeconds);
			}
			row.setFloat("fuel_flow_kg_sec" , fuel_flow_kg_seconds);
		}
		logger.info( this.fuelDataTable.print(10));
	}

	/**
	 * one shot , before start the big loop on each of the 131530 fuel train rows
	 */
	public void createExtendedEngineeringFeaturesColumns() {

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

		//=======================================
		// compute distance flown from fuel start to end
		DoubleColumn aircraft_distance_flown_start_end_column = DoubleColumn.create("aircraft_distance_flown_start_end_Nm");
		this.fuelDataTable.addColumns(aircraft_distance_flown_start_end_column);

		//============================
		// distance relative from origin airport to fuel start end
		// compute distance flown from origin airport to fuel start
		DoubleColumn aircraft_distance_flown_origin_start_column = DoubleColumn.create("aircraft_distance_flown_origin_start_Nm");
		this.fuelDataTable.addColumns(aircraft_distance_flown_origin_start_column);

		// compute distance flown from origin airport to fuel end
		DoubleColumn aircraft_distance_flown_origin_end_column = DoubleColumn.create("aircraft_distance_flown_origin_end_Nm");
		this.fuelDataTable.addColumns(aircraft_distance_flown_origin_end_column);

		//============================================
		// distance from fuel start end to destination
		// compute distance still to be flown from fuel start to destination airport
		DoubleColumn aircraft_distance_to_be_flown_start_destination_column = DoubleColumn.create("aircraft_distance_to_be_flown_start_destination_Nm");
		this.fuelDataTable.addColumns(aircraft_distance_to_be_flown_start_destination_column);

		// compute distance still to be flown from fuel end to destination airport
		DoubleColumn aircraft_distance_to_be_flown_end_destination_column = DoubleColumn.create("aircraft_distance_to_be_flown_end_destination_Nm");
		this.fuelDataTable.addColumns(aircraft_distance_to_be_flown_end_destination_column);

		//============================================
		// ------------- altitude
		DoubleColumn aircraft_altitude_start_column = DoubleColumn.create("aircraft_altitude_ft_at_fuel_start");
		this.fuelDataTable.addColumns(aircraft_altitude_start_column);

		DoubleColumn aircraft_altitude_end_column = DoubleColumn.create("aircraft_altitude_ft_at_fuel_end");
		this.fuelDataTable.addColumns(aircraft_altitude_end_column);

		//============================================
		// delta altitudes
		// delta altitude origin airport to aircraft altitude at fuel start
		DoubleColumn delta_altitude_origin_start_column = DoubleColumn.create("aircraft_delta_altitude_ft_origin_fuel_start");
		this.fuelDataTable.addColumns(delta_altitude_origin_start_column);

		// delta altitude origin airport to aircraft altitude at fuel end
		DoubleColumn delta_altitude_origin_end_column = DoubleColumn.create("aircraft_delta_altitude_ft_origin_end_start");
		this.fuelDataTable.addColumns(delta_altitude_origin_end_column);

		// delta altitude aircraft altitude at fuel start to destination airport altitude
		DoubleColumn delta_altitude_start_destination_column = DoubleColumn.create("aircraft_delta_altitude_ft_start_destination");
		this.fuelDataTable.addColumns(delta_altitude_start_destination_column);

		// delta altitude aircraft altitude at fuel end to destination airport altitude
		DoubleColumn delta_altitude_end_destination_column = DoubleColumn.create("aircraft_delta_altitude_ft_end_destination");
		this.fuelDataTable.addColumns(delta_altitude_end_destination_column);

		//============================================
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

		//=============================================================
		// time difference between takeoff and fuel burnt start and end
		LongColumn fuel_burnt_start_relative_to_takeoff_sec_column = LongColumn.create("fuel_burnt_start_relative_to_takeoff_sec");
		this.fuelDataTable.addColumns(fuel_burnt_start_relative_to_takeoff_sec_column);

		LongColumn fuel_burnt_end_relative_to_takeoff_sec_column = LongColumn.create("fuel_burnt_end_relative_to_takeoff_sec");
		this.fuelDataTable.addColumns(fuel_burnt_end_relative_to_takeoff_sec_column);

		LongColumn fuel_burnt_start_relative_to_landed_sec_column = LongColumn.create("fuel_burnt_start_relative_to_landed_sec");
		this.fuelDataTable.addColumns(fuel_burnt_start_relative_to_landed_sec_column );

		LongColumn fuel_burnt_end_relative_to_landed_sec_column = LongColumn.create("fuel_burnt_end_relative_to_landed_sec");
		this.fuelDataTable.addColumns(fuel_burnt_end_relative_to_landed_sec_column );
	}

	/**
	 * row level function
	 * @return
	 */
	public Double leaveItMissingIfApplicable( Row row , final String columnName ) {

		int columnIndex = this.getFuelDataTable().columnIndex(columnName);
		Column<?> column = (Column<?>) this.getFuelDataTable().column(columnIndex);
		int rowNumber = row.getRowNumber();
		if ( column.isMissing(rowNumber)) {
			return (Double)null;
		} else {
			DoubleColumn columnDouble = this.getFuelDataTable().doubleColumn(columnIndex);
			return  columnDouble.get(rowNumber);
		}
	}

	public void setDouble( Row row , final String columnName , final Double doubleValueWithPotentialNull) {

		if ( (Double)doubleValueWithPotentialNull == null ) {
			row.doubleColumnMap.get(columnName).setMissing(row.getIndex(row.getRowNumber()));
		} else {
			row.doubleColumnMap.get(columnName).set(row.getIndex(row.getRowNumber()), doubleValueWithPotentialNull);
		}
	}

	private void manageAltitudes( Row row , final Instant start , final Instant end ) {

		//===============================================
		// altitude
		Double aircraft_altitude_ft_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  start);
		row.setDouble("aircraft_altitude_ft_at_fuel_start" , aircraft_altitude_ft_fuel_start);

		Double aircraft_altitude_ft_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  end);
		row.setDouble("aircraft_altitude_ft_at_fuel_end" , aircraft_altitude_ft_fuel_end);

		// ==============================================
		// delta altitude
		float airport_origin_elevation_ft = row.getFloat("origin_elevation_feet");
		// delta altitude origin airport to aircraft altitude at fuel start
		if (( aircraft_altitude_ft_fuel_start == null) || ( ((Float)airport_origin_elevation_ft) == null ) ){
			row.setDouble("aircraft_delta_altitude_ft_origin_fuel_start" , null);
		} else {
			row.setDouble("aircraft_delta_altitude_ft_origin_fuel_start" , (aircraft_altitude_ft_fuel_start - airport_origin_elevation_ft));
		}

		if (( aircraft_altitude_ft_fuel_end == null) || ( ((Float)airport_origin_elevation_ft) == null ) ){
			row.setDouble("aircraft_delta_altitude_ft_origin_end_start" , null);
		} else {
			// delta altitude origin airport to aircraft altitude at fuel end
			row.setDouble("aircraft_delta_altitude_ft_origin_end_start" , (aircraft_altitude_ft_fuel_end - airport_origin_elevation_ft));
		}

		//============================================================
		// delta altitude aircraft altitude at fuel start to destination airport altitude
		float airport_destination_elevation_ft = row.getFloat("destination_elevation_feet");

		if (( aircraft_altitude_ft_fuel_start == null) || ( (Float)(airport_destination_elevation_ft) == null)) {
			row.setDouble("aircraft_delta_altitude_ft_start_destination" , null);
		} else {
			row.setDouble("aircraft_delta_altitude_ft_start_destination" , (aircraft_altitude_ft_fuel_start - airport_destination_elevation_ft));
		}

		if (( aircraft_altitude_ft_fuel_end == null) || ( ((Float)(airport_destination_elevation_ft)) == null) ) {
			row.setDouble("aircraft_delta_altitude_ft_end_destination" , null);
		} else {
			// delta altitude aircraft altitude at fuel end to destination airport altitude
			row.setDouble("aircraft_delta_altitude_ft_end_destination" , (aircraft_altitude_ft_fuel_end - airport_destination_elevation_ft));
		}

		//========================================
		// computed vertical rate feet per minutes
		long time_diff_sec = row.getLong("time_diff_seconds");

		// warning -> do not used absolute because this feature can be Positive or Negative
		if (( aircraft_altitude_ft_fuel_end == null )||( aircraft_altitude_ft_fuel_start == null )|| ( ((Long) time_diff_sec) == null)) {
			row.setDouble("aircraft_computed_vertical_rate_ft_min" , null);

		} else {
			double computed_vertical_ft_min_rate = (aircraft_altitude_ft_fuel_end - aircraft_altitude_ft_fuel_start)/ (float)(time_diff_sec / 60.0);
			row.setDouble("aircraft_computed_vertical_rate_ft_min" , computed_vertical_ft_min_rate);
		}
	}

	private void manageTrackAngles ( Row row , final Instant start , final Instant end ) {

		//=======================================
		// ground speed at fuel start and at fuel end
		Double groundSpeed_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("groundspeed",  start);
		row.setDouble("aircraft_groundspeed_kt_at_fuel_start" , groundSpeed_start);

		Double groundSpeed_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("groundspeed" ,  end);
		row.setDouble("aircraft_groundspeed_kt_at_fuel_end" , groundSpeed_end);

		//=======================================
		// track angle degrees as fuel start and at fuel end
		Double track_angle_deg_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("track" ,  start);
		row.setDouble("aircraft_track_angle_deg_at_fuel_start" , track_angle_deg_start);

		Double track_angle_deg_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("track" ,  end);
		row.setDouble("aircraft_track_angle_deg_at_fuel_end" ,  track_angle_deg_end);

		//=======================================
		// ground speed X and Y projected components
		if (( groundSpeed_start == null ) || ( track_angle_deg_start == null)){
			row.setDouble("aircraft_groundspeed_kt_X_at_fuel_start" , null);
			row.setDouble("aircraft_groundspeed_kt_Y_at_fuel_start" , null);
		} else {
			row.setDouble("aircraft_groundspeed_kt_X_at_fuel_start" , groundSpeed_start * Math.cos(Math.toRadians(track_angle_deg_start)));
			row.setDouble("aircraft_groundspeed_kt_Y_at_fuel_start" , groundSpeed_start * Math.sin(Math.toRadians(track_angle_deg_start)));
		}

		// ground speed X and Y projected components
		if (( groundSpeed_end == null ) || (track_angle_deg_end == null)){
			row.setDouble("aircraft_groundspeed_kt_X_at_fuel_end" , null);
			row.setDouble("aircraft_groundspeed_kt_Y_at_fuel_end" , null);
		} else {
			row.setDouble("aircraft_groundspeed_kt_X_at_fuel_end" , groundSpeed_end * Math.cos(Math.toRadians(track_angle_deg_end) ) );
			row.setDouble("aircraft_groundspeed_kt_Y_at_fuel_end" , groundSpeed_end * Math.sin(Math.toRadians(track_angle_deg_end)));
		}

		//===========================================
		// track angle radians at fuel start
		if (track_angle_deg_start == null) {
			row.setDouble("aircraft_track_angle_rad_at_fuel_start" , null );
		} else {
			row.setDouble("aircraft_track_angle_rad_at_fuel_start" , Math.toRadians(track_angle_deg_start) );
		}	
		// track angle at fuel end
		if (track_angle_deg_end== null) {
			row.setDouble("aircraft_track_angle_rad_at_fuel_end" ,  null);
		} else {
			row.setDouble("aircraft_track_angle_rad_at_fuel_end" ,  Math.toRadians(track_angle_deg_end));
		}
	}

	private void manageSpeeds( Row row , final Instant start , final Instant end ) {

		//=======================================
		// mach
		Double mach_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("mach" ,  start);
		row.setDouble("aircraft_mach_at_fuel_start" , mach_start);

		Double mach_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("mach" ,  end);
		row.setDouble("aircraft_mach_at_fuel_end" , mach_end);

		//=======================================
		// TAS - or use mach if mach not missing / hole / nan
		Double TAS_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("TAS" ,  start);
		//Double TAS_start = this.leaveItMissingIfApplicable( row , "aircraft_TAS_at_fuel_start");
		if ((Double)TAS_start == null) {
			if ( (Double)mach_start == null ) {
				row.setMissing("aircraft_TAS_at_fuel_start");
			} else {
				String speed_units = "kt";
				String alt_units = "ft";
				Double aircraft_altitude_ft_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  start);
				if ( aircraft_altitude_ft_fuel_start == null) {
					row.setMissing("aircraft_TAS_at_fuel_start");
				} else {
					TAS_start = this.airSpeedConverter.mach2tas(mach_start, aircraft_altitude_ft_fuel_start, speed_units, alt_units);
					row.setDouble("aircraft_TAS_at_fuel_start" , TAS_start);
				}
			}
		} else {
			row.setDouble("aircraft_TAS_at_fuel_start" , TAS_start);
		}
		//======================================
		// TAS at fuel end
		Double TAS_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("TAS" ,  end);
		//Double TAS_end = this.leaveItMissingIfApplicable( row , "aircraft_TAS_at_fuel_end");

		if ((Double)TAS_end == null) {
			if ( (Double)mach_end == null ) {
				row.setMissing("aircraft_TAS_at_fuel_end");
			} else {
				String speed_units = "kt";
				String alt_units = "ft";
				Double aircraft_altitude_ft_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  start);
				if (aircraft_altitude_ft_fuel_end == null) {
					row.setDouble("aircraft_TAS_at_fuel_end" , null);
				} else {
					TAS_end = this.airSpeedConverter.mach2tas(mach_end, aircraft_altitude_ft_fuel_end, speed_units, alt_units);
					row.setDouble("aircraft_TAS_at_fuel_end" , TAS_end);
				}
			}
		} else {
			row.setDouble("aircraft_TAS_at_fuel_end" , TAS_end);
		}

		//=======================================
		// CAS at fuel start - leave it missing if it is missing
		Double CAS_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("CAS" ,  start);
		//Double CAS_start = this.leaveItMissingIfApplicable( row , "aircraft_CAS_at_fuel_start");
		if ((Double)CAS_start == null) {
			if ( (Double)mach_start == null ) {
				row.setMissing("aircraft_CAS_at_fuel_start");
			} else {
				String speed_units = "kt";
				String alt_units = "ft";
				Double aircraft_altitude_ft_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  start);
				if ( aircraft_altitude_ft_fuel_start == null) {
					row.setDouble("aircraft_CAS_at_fuel_start" , null);
				} else {
					CAS_start = this.airSpeedConverter.mach2cas(mach_start, aircraft_altitude_ft_fuel_start, speed_units, alt_units);
					row.setDouble("aircraft_CAS_at_fuel_start" , CAS_start);
				}
			}
		} else {
			row.setDouble("aircraft_CAS_at_fuel_start" , CAS_start);
		}

		//===========================================
		// CAS at fuel end
		Double CAS_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("CAS" ,  end);
		//Double CAS_end = this.leaveItMissingIfApplicable( row , "aircraft_CAS_at_fuel_end");

		if ((Double)CAS_end == null) {
			if ( (Double)mach_end == null ) {
				row.setMissing("aircraft_CAS_at_fuel_end");
			} else {
				String speed_units = "kt";
				String alt_units = "ft";
				Double aircraft_altitude_ft_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  start);
				if ( aircraft_altitude_ft_fuel_end == null) {
					row.setMissing("aircraft_CAS_at_fuel_end");
				} else {
					CAS_end = this.airSpeedConverter.mach2cas(mach_end, aircraft_altitude_ft_fuel_end, speed_units, alt_units);
					row.setDouble("aircraft_CAS_at_fuel_end" , CAS_end);
				}
			}
		} else {
			row.setDouble("aircraft_CAS_at_fuel_end" , CAS_end);
		}
	}

	/**
	 * this method is launched inside an Executor execute from java concurrency
	 * row -> current row in Fuel Table
	 * @param row
	 * @throws IOException 
	 * @throws utils.CustomException 
	 */
	public void extendOneFuelRowStartEndInstantWithFlightData (final LocalDateTime startTime, Row row ) throws IOException, utils.CustomException {

		LocalDateTime nowDateTime = LocalDateTime.now();
		long hours = ChronoUnit.HOURS.between(startTime, nowDateTime);
		long minutes = ChronoUnit.MINUTES.between(startTime, nowDateTime) % 60;
		long seconds = ChronoUnit.SECONDS.between(startTime, nowDateTime) % 60;

		Instant start = row.getInstant("start");
		Instant end = row.getInstant("end");

		String flight_id = row.getString ("flight_id");
		logger.info(flight_id);
		FlightData flightData = new FlightData( train_rank_value , flight_id );
		// 27th October 2025 - use new stream reader capable of filling empty values
		// read one flight data parquet file
		try {
			// reading with stream allows to keep missing values as holes
			flightData.readParquetWithStream();
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
			
		} catch (utils.CustomException e) {
			e.printStackTrace();
			throw e;
		}
		if ( flightData.getFlightDataTable().isEmpty() ) {
			logger.info("flight data for flight id = <<" + flight_id + ">> is empty");
			return ;
		} else {

			// one set of interpolation function for each loaded flight data frame
			this.flightDataInterpolation.buildInterpolationFunctions(flightData.getFlightDataTable());

			System.out.println("--------------------------------------");
			System.out.println("----------------- hours = " + hours + " -> minutes = " + minutes + " -> seconds = " + seconds);
			System.out.println("----------------- row count = "+ row.getRowNumber() + " / max = " + this.fuelDataTable.rowCount() + " ---------------------");
			System.out.println("--------------------------------------");

			// get interpolated value from the flight data -> hence latitude and longitude in degrees
			Double ac_lat_deg_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("latitude" , start);
			Double ac_lon_deg_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("longitude" ,start);

			// get interpolated value from the flight data -> hence latitude or longitude in degrees
			Double ac_lat_deg_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("latitude"  ,end);
			Double ac_lon_deg_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("longitude"  ,end);

			row.setDouble("aircraft_latitude_deg_at_fuel_start" , ac_lat_deg_fuel_start);
			if ( ac_lat_deg_fuel_start == null ) {
				row.setDouble("aircraft_latitude_rad_at_fuel_start" , null);
			} else {
				row.setDouble("aircraft_latitude_rad_at_fuel_start" , Math.toRadians(ac_lat_deg_fuel_start ));
			}

			row.setDouble("aircraft_longitude_deg_at_fuel_start" , ac_lon_deg_fuel_start);
			if ( ac_lon_deg_fuel_start == null ) {
				row.setDouble("aircraft_longitude_rad_at_fuel_start" , null);
			} else {
				row.setDouble("aircraft_longitude_rad_at_fuel_start" , Math.toRadians( ac_lon_deg_fuel_start ));
			}

			row.setDouble("aircraft_latitude_deg_at_fuel_end" , ac_lat_deg_fuel_end);
			if ( ac_lat_deg_fuel_end == null ) {
				row.setDouble("aircraft_latitude_rad_at_fuel_end" , null);
			} else {
				row.setDouble("aircraft_latitude_rad_at_fuel_end" , Math.toRadians( ac_lat_deg_fuel_end ) );
			}

			row.setDouble("aircraft_longitude_deg_at_fuel_end" , ac_lon_deg_fuel_end);
			if ( ac_lon_deg_fuel_end == null ) {
				row.setDouble("aircraft_longitude_rad_at_fuel_end" , null);
			} else {
				row.setDouble("aircraft_longitude_rad_at_fuel_end" , Math.toRadians(ac_lon_deg_fuel_end ));
			}

			//=================================================================
			// compute distance flown in Nautical miles between fuel start and fuel end
			if (( ac_lat_deg_fuel_start == null )|| (ac_lon_deg_fuel_start == null)||(ac_lat_deg_fuel_end==null)||(ac_lon_deg_fuel_end==null)) {
				row.setDouble("aircraft_distance_flown_start_end_Nm" , null);
			} else {
				double distanceFlownNmBetweenStartEnd = Utils.calculateHaversineDistanceNauticalMiles( ac_lat_deg_fuel_start, ac_lon_deg_fuel_start, 
						ac_lat_deg_fuel_end, ac_lon_deg_fuel_end); 
				row.setDouble("aircraft_distance_flown_start_end_Nm" , distanceFlownNmBetweenStartEnd);
			}                                                                                               

			//============================================================
			// added 3rd November 2025
			// compute distance flown in Nm between origin airport and aircraft position at fuel start
			Double origin_latitude_deg = row.getDouble("origin_latitude_deg");
			Double origin_longitude_deg = row.getDouble("origin_longitude_deg");

			if (( origin_latitude_deg==null)||(origin_longitude_deg==null)||(ac_lat_deg_fuel_start==null)||(ac_lon_deg_fuel_start==null)){
				row.setDouble("aircraft_distance_flown_origin_start_Nm", null);
			}else {
				Double distanceNmFlownOriginToStart = Utils.calculateHaversineDistanceNauticalMiles(
						origin_latitude_deg, origin_longitude_deg, ac_lat_deg_fuel_start, ac_lon_deg_fuel_start);
				row.setDouble("aircraft_distance_flown_origin_start_Nm", distanceNmFlownOriginToStart);
			}

			if ( (origin_latitude_deg==null)||(origin_longitude_deg==null)||(ac_lat_deg_fuel_end==null)||(ac_lon_deg_fuel_end==null)){
				row.setDouble("aircraft_distance_flown_origin_end_Nm", null);
			} else {
				// compute distance flown in Nm between origin airport and aircraft position at fuel end
				double distanceFlownNmOriginToEnd = Utils.calculateHaversineDistanceNauticalMiles(
						origin_latitude_deg, origin_longitude_deg, ac_lat_deg_fuel_end, ac_lon_deg_fuel_end);
				row.setDouble("aircraft_distance_flown_origin_end_Nm", distanceFlownNmOriginToEnd);
			}

			// compute distance to be flown in Nm between aircraft position at fuel start and destination airport
			Double destination_latitude_deg = row.getDouble("destination_latitude_deg");
			Double destination_longitude_deg = row.getDouble("destination_longitude_deg");

			if ( (ac_lat_deg_fuel_start==null)||(ac_lon_deg_fuel_start==null)||(destination_latitude_deg==null)||(destination_longitude_deg==null)) {
				row.setDouble("aircraft_distance_to_be_flown_start_destination_Nm", null);
			} else {
				Double distanceToBeFlownNmStartToDestination = Utils.calculateHaversineDistanceNauticalMiles(
						ac_lat_deg_fuel_start, ac_lon_deg_fuel_start, destination_latitude_deg, destination_longitude_deg);
				row.setDouble("aircraft_distance_to_be_flown_start_destination_Nm", distanceToBeFlownNmStartToDestination);
			}

			// compute distance to be flown in Nm between aircraft position at fuel end and destination airport
			if ( (ac_lat_deg_fuel_end==null)||(ac_lon_deg_fuel_end==null)||(destination_latitude_deg==null)||(destination_longitude_deg==null)) {
				row.setDouble("aircraft_distance_to_be_flown_end_destination_Nm", null);
			} else {
				Double distanceToBeFlownNmEndToDestination = Utils.calculateHaversineDistanceNauticalMiles(
						ac_lat_deg_fuel_end, ac_lon_deg_fuel_end, destination_latitude_deg, destination_longitude_deg);
				row.setDouble("aircraft_distance_to_be_flown_end_destination_Nm", distanceToBeFlownNmEndToDestination);
			}

			// manage altitudes 
			this.manageAltitudes( row ,  start , end );

			// manage everything related to track angles
			this.manageTrackAngles(row, start, end);


			//=======================================
			// vertical rate
			Double vertical_rate_ft_min_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("vertical_rate" ,  start);
			row.setDouble("aircraft_vertical_rate_ft_min_at_fuel_start" , vertical_rate_ft_min_start);

			Double vertical_rate_ft_min_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("vertical_rate" ,  end);
			row.setDouble("aircraft_vertical_rate_ft_min_at_fuel_end" , vertical_rate_ft_min_end);

			// every thing related to mach , TAS and CAS
			this.manageSpeeds( row , start, end );


			//================================
			// duration between flight takeoff and fuel burnt start end
			// duration between fuel burnt start and end ... and flight landed Instant
			int idx = row.getInt("idx");

			Instant takeoff = row.getInstant("takeoff");
			Instant landed = row.getInstant("landed");
			assert takeoff.isBefore(landed);

			if ( ( takeoff != null ) && takeoff.isBefore(landed) ) {
				assert takeoff.isBefore(landed);
			}	else {
				this.errorsMap.put(idx, new ArrayList<>(List.of(row.getString("flight_id") , takeoff.toString() , landed.toString())));
				//System.out.println(row.getRowNumber());
			}
			// takeoff versus fuel start
			if ( ( takeoff != null ) && takeoff.isBefore(start) ) {
				//assert takeoff.isBefore(start);
				long duration_sec = Duration.between(takeoff, start).toSeconds();
				row.setLong("fuel_burnt_start_relative_to_takeoff_sec" , duration_sec);
			} else {
				System.out.println("Error takeoff = " + takeoff + " -- not before fuel burnt start = " + start);
				row.setLong("fuel_burnt_start_relative_to_takeoff_sec" , null);
			}
			// takeoff versus fuel end
			if ( ( takeoff != null ) && takeoff.isBefore(end) ) {
				//assert takeoff.isBefore(end);
				long duration_sec = Duration.between(takeoff, end).toSeconds();
				row.setLong("fuel_burnt_end_relative_to_takeoff_sec" , duration_sec);
			} else {
				System.out.println("Error takeoff = " + takeoff + " -- not before fuel burnt end = " + end);
				row.setLong("fuel_burnt_end_relative_to_takeoff_sec" , null);
			}
			// start versus landed
			if ( ( landed != null ) && ( landed.getEpochSecond() > 0 )) {
				if ( start.isBefore(landed) ) {
					long duration_sec = Duration.between(start, landed).toSeconds();
					row.setLong("fuel_burnt_start_relative_to_landed_sec" , duration_sec);
				}
			} else {
				row.setLong("fuel_burnt_start_relative_to_landed_sec" , null);
				System.out.println("Error fuel burnt end = " + end + " -- not before landed = " + landed);
			}
			// end versus landed
			if ( ( landed != null ) && ( landed.getEpochSecond() > 0 )) {
				if ( end.isBefore(landed) ) {
					long duration_sec = Duration.between(end, landed).toSeconds();
					row.setLong("fuel_burnt_end_relative_to_landed_sec" , duration_sec);
				}
			} else {
				row.setLong("fuel_burnt_end_relative_to_landed_sec" , null);
				System.out.println("Error fuel burnt end = " + end + " -- not before landed = " + landed);
			}

		}
	}

	/**
	 * use interpolation function generated beforehand
	 * @param maxToBeComputedRow
	 * @throws IOException
	 * @throws utils.CustomException 
	 */
	protected void extendFuelStartEndInstantsWithFlightData( ) throws IOException, utils.CustomException {

		// find the nearest instant from a fuel table of a flight id
		// given a fuel start or stop instant
		train_rank_final train_rank_value = this.getTrain_rank_value();
		long maxToBeComputedRow = this.getMaxToBeComputedRow();

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

			//logger.info(flightData.getFlightDataTable().shape() );
			//logger.info(flightData.getFlightDataTable().structure().print() );

			// one set of interpolation function for each loaded flight data frame
			this.flightDataInterpolation.buildInterpolationFunctions(flightData.getFlightDataTable());

			System.out.println("--------------------------------------");
			System.out.println("----------------- row count = "+ counter + " / max = " + this.fuelDataTable.rowCount() + " ---------------------");
			System.out.println("--------------------------------------");

			Double ac_lat_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("latitude" , start);
			Double ac_lon_fuel_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("longitude" ,start);

			Double ac_lat_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("latitude"  ,end);
			Double ac_lon_fuel_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("longitude"  ,end);

			row.setDouble("aircraft_latitude_deg_at_fuel_start" , ac_lat_fuel_start);
			row.setDouble("aircraft_latitude_rad_at_fuel_start" , (ac_lat_fuel_start * Math.PI)/180.0);

			row.setDouble("aircraft_longitude_deg_at_fuel_start" , ac_lon_fuel_start);
			row.setDouble("aircraft_longitude_rad_at_fuel_start" , ( ac_lon_fuel_start * Math.PI)/180.0);

			row.setDouble("aircraft_latitude_deg_at_fuel_end" , ac_lat_fuel_end);
			row.setDouble("aircraft_latitude_rad_at_fuel_end" , ( ac_lat_fuel_end * Math.PI)/180.0);

			row.setDouble("aircraft_longitude_deg_at_fuel_end" , ac_lon_fuel_end);
			row.setDouble("aircraft_longitude_rad_at_fuel_end" , (ac_lon_fuel_end * Math.PI)/180.0);

			// compute distance flown in Nautical miles between fuel start and fuel end
			Double distanceFlownBetweenStartEnd = Utils.calculateHaversineDistanceNauticalMiles( ac_lat_fuel_start, ac_lon_fuel_start, ac_lat_fuel_end, ac_lon_fuel_end); 
			row.setDouble("aircraft_distance_flown_start_end_Nm" , distanceFlownBetweenStartEnd);

			Double altitude_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  start);
			row.setDouble("aircraft_altitude_ft_at_fuel_start" , altitude_start);

			Double altitude_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude" ,  end);
			row.setDouble("aircraft_altitude_ft_at_fuel_end" , altitude_end);

			// computed vertical rate feet per minutes
			long time_diff_sec = row.getLong("time_diff_seconds");

			double computed_vertical_rate = (altitude_end - altitude_start)/ (float)time_diff_sec;
			row.setDouble("aircraft_computed_vertical_rate_ft_min" , computed_vertical_rate);

			// ground speed
			Double groundSpeed_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("groundspeed",  start);
			row.setDouble("aircraft_groundspeed_kt_at_fuel_start" , groundSpeed_start);

			Double groundSpeed_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("groundspeed" ,  end);
			row.setDouble("aircraft_groundspeed_kt_at_fuel_end" , groundSpeed_end);

			// track angle degrees
			Double track_angle_deg_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("track" ,  start);
			row.setDouble("aircraft_track_angle_deg_at_fuel_start" , track_angle_deg_start);

			Double track_angle_deg_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("track" ,  end);
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
			Double vertical_rate_ft_min_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("vertical_rate" ,  start);
			row.setDouble("aircraft_vertical_rate_ft_min_at_fuel_start" , vertical_rate_ft_min_start);

			Double vertical_rate_ft_min_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("vertical_rate" ,  end);
			row.setDouble("aircraft_vertical_rate_ft_min_at_fuel_end" , vertical_rate_ft_min_end);

			// mach
			Double mach_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("mach" ,  start);
			row.setDouble("aircraft_mach_at_fuel_start" , mach_start);

			Double mach_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("mach" ,  end);
			row.setDouble("aircraft_mach_at_fuel_end" , mach_end);

			// TAS
			Double TAS_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("TAS" ,  start);
			row.setDouble("aircraft_TAS_at_fuel_start" , TAS_start);

			Double TAS_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("TAS" ,  end);
			row.setDouble("aircraft_TAS_at_fuel_end" , TAS_end);

			// CAS
			Double CAS_start = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("CAS" ,  start);
			row.setDouble("aircraft_CAS_at_fuel_start" , CAS_start);

			Double CAS_end = this.flightDataInterpolation.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("CAS" ,  end);
			row.setDouble("aircraft_CAS_at_fuel_end" , CAS_end);


			// duration between flight takeoff and fuel burnt start end
			// duration between fuel burnt start and end ... and flight landed Instant
			int idx = row.getInt("idx");

			Instant takeoff = row.getInstant("takeoff");
			Instant landed = row.getInstant("landed");
			assert takeoff.isBefore(landed);

			if ( ( takeoff != null ) && takeoff.isBefore(landed) ) {
				assert takeoff.isBefore(landed);
			}	else {
				this.errorsMap.put(idx, new ArrayList<>(List.of( row.getString("flight_id") ,    takeoff.toString() ,landed.toString() )));
				//System.out.println(row.getRowNumber());
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
				if ( start.isBefore(landed) ) {
					long duration_sec = Duration.between(start, landed).toSeconds();
					row.setLong("fuel_burnt_start_relative_to_landed_sec" , duration_sec);
				}
			} else {
				System.out.println("Error fuel burnt end = " + end + " -- not before landed = " + landed);
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
				this.errorsMap.put(idx, new ArrayList<>(List.of( row.getString("flight_id")  ,  takeoff.toString()  ,landed.toString()  )));
				//System.out.println(row.getRowNumber());
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
				if ( start.isBefore(landed) ) {
					long duration_sec = Duration.between(start, landed).toSeconds();
					row.setLong("fuel_burnt_start_relative_to_landed_sec" , duration_sec);
				}
			} else {
				System.out.println("Error fuel burnt end = " + end + " -- not before landed = " + landed);
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

	public void setTrain_rank_value(train_rank_final train_rank_value) {
		this.train_rank_value = train_rank_value;
	}

	public train_rank_final getTrain_rank_value() {
		return train_rank_value;
	}

	public long getMaxToBeComputedRow() {
		return maxToBeComputedRow;
	}

	public void setMaxToBeComputedRow(long maxToBeComputedRow) {
		this.maxToBeComputedRow = maxToBeComputedRow;
	}

	@Override
	public void run() {

	}

	public void setFuelDataTable(Table fuelDataTable) {
		this.fuelDataTable = fuelDataTable;
	}

	public Table getFuelDataTable() {
		return this.fuelDataTable;
	}

	public Map<Integer, ArrayList<String>> getErrorsMap() {
		return errorsMap;
	}
}

