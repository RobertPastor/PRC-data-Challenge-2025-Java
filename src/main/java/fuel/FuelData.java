package fuel;


import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.io.FileSystemOutputFile;

import aircrafts.AircraftsData;
import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import folderDiscovery.FolderDiscovery;
import fuel.FuelDataSchema.FuelDataRecord;
import fuel.FuelDataSchema.FuelExtendedDataRecord;
import utils.Utils;

public class FuelData extends FuelDataTable {

	private static final Logger logger = Logger.getLogger(FuelData.class.getName());

	/**
	 * max computed row used during debugging to reduce the number of analyzed rows from the Fuel table
	 * @param value
	 * @param maxToBeComputedRow
	 */
	public FuelData( train_rank_final train_rank_value , final long maxToBeComputedRow ) {
		super(train_rank_value , maxToBeComputedRow);
		logger.info("--- constructor for <<" + this.getTrain_rank_value() + ">> ---");
	}

	/**
	 * main preparation method before looping through fuel rows and interpolating the flight aircraft position
	 * @param maxToBeComputedRow
	 * @throws IOException
	 * @throws utils.CustomException 
	 */
	public void prepareBeforeMergeFueltoOtherData (final long maxToBeComputedRow , final String aircraft_type_code ) throws IOException, utils.CustomException {

		train_rank_final train_rank_value = this.getTrain_rank_value();

		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();
		
		AircraftsData aircraftsData = new AircraftsData();
		aircraftsData.readExcelFile();

		FlightListData flightListData = new FlightListData(train_rank_value, aircraft_type_code);
		flightListData.readParquet();
		flightListData.extendWithFlightDateData();

		logger.info( flightListData.getFlightListDataTable().shape() );

		flightListData.extendWithAirportData( airportsData );
		//flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();

		//logger.info(flightListData.getFlightListDataTable().shape());
		//logger.info(flightListData.getFlightListDataTable().structure().print());

		flightListData.extendWithAircraftsData( aircraftsData );
		//flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();

		logger.info(flightListData.getFlightListDataTable().shape());
		logger.info("fuel data table - row count = " +  this.getFuelDataTable().rowCount());

		// extend fuel with end minus start differences
		this.extendFuelWithEndStartDifference();

		//logger.info(this.getFuelDataTable().shape());
		//logger.info(this.getFuelDataTable().structure().print());

		// extend fuel with fuel flow in Kilograms per seconds
		this.extendFuelFlowKgSeconds();
		//logger.info(this.getFuelDataTable().shape());
		//logger.info(this.getFuelDataTable().structure().print());

		// merge fuel with flight list
		this.extendFuelWithFlightListData( flightListData.getFlightListDataTable() ) ;
		//logger.info(this.getFuelDataTable().shape());
		//logger.info(this.getFuelDataTable().structure().print());

		// as flight take-off and landed are now available from flight list 
		// use them to compute relative delta from burnt start and stop
		// extend fuel with start end differences from takeoff
		this.extendRelativeStartEndFromFlightTakeoff();

		//logger.info(this.getFuelDataTable().shape());
		//logger.info(this.getFuelDataTable().structure().print());

		// extend fuel with flight data - there is a "merge" between fuel and flight data
		this.extendFuelStartEndInstantsWithFlightData( );

		// generate final parquet file with extended fuel dataframe
		//this.generateParquetFileFor();

		//this.generateListOfErrors();

	}

	public void generateParquetFileFor( ) throws IOException {

		logger.info("--- generate Parquet file <<" + this.getTrain_rank_value() + ">> ---");

		String currentDateTimeAsStr = Utils.getCurrentDateTimeasStr();
		String fileName  = "ExtendedFuel_" + this.getTrain_rank_value() + "_" + currentDateTimeAsStr + ".parquet";

		String folderStr = FolderDiscovery.getTrainRankOutputfolderStr();

		Path path = Paths.get(folderStr , fileName );
		File file = path.toFile();

		logger.info(fileName);
		logger.info(file.getAbsolutePath());

		try {
			List<FuelExtendedDataRecord> fuelRecords = new ArrayList<FuelExtendedDataRecord>();
			this.getFuelDataTable().stream().forEach(row -> {
				FuelExtendedDataRecord record = new FuelExtendedDataRecord(
						row.getInt("idx"),
						row.getString("flight_id") , 

						row.getInstant("start"),
						row.getInstant("end") ,

						row.getLong("time_diff_seconds") , 
						row.getFloat("fuel_flow_kg_sec") ,

						//============= aircraft positions
						// informations taken from the flight data between fuel start and fuel end
						row.getDouble("aircraft_latitude_deg_at_fuel_start"),
						row.getDouble("aircraft_longitude_deg_at_fuel_start"),

						// latitude longitude RADIANS at fuel start
						row.getDouble("aircraft_latitude_rad_at_fuel_start"),
						row.getDouble("aircraft_longitude_rad_at_fuel_start"),

						// latitude and longitude DEGREES at fuel end
						row.getDouble("aircraft_latitude_deg_at_fuel_end"),
						row.getDouble("aircraft_longitude_deg_at_fuel_end"),

						// latitude and longitude RADIANS at fuel end
						row.getDouble("aircraft_latitude_rad_at_fuel_end"),
						row.getDouble("aircraft_longitude_rad_at_fuel_end"),

						//=================================
						// distances
						row.getDouble("aircraft_distance_flown_start_end_Nm"),
						row.getDouble("aircraft_distance_flown_origin_start_Nm"),
						row.getDouble("aircraft_distance_flown_origin_end_Nm"),
						row.getDouble("aircraft_distance_to_be_flown_start_destination_Nm"),
						row.getDouble("aircraft_distance_to_be_flown_end_destination_Nm"),

						//==================================
						// absolute altitudes
						// altitude at fuel start and stop
						row.getDouble("aircraft_altitude_ft_at_fuel_start"),
						row.getDouble("aircraft_altitude_ft_at_fuel_end"),

						//==================================
						// delta altitudes
						row.getDouble("aircraft_delta_altitude_ft_origin_fuel_start"),
						row.getDouble("aircraft_delta_altitude_ft_origin_end_start"),
						row.getDouble("aircraft_delta_altitude_ft_start_destination"),
						row.getDouble("aircraft_delta_altitude_ft_end_destination"),

						//=====================================================
						// ground speed
						row.getDouble("aircraft_groundspeed_kt_at_fuel_start"),
						row.getDouble("aircraft_groundspeed_kt_at_fuel_end"),

						// ground speed X and Y components using the cosine and sine of the track angle
						row.getDouble("aircraft_groundspeed_kt_X_at_fuel_start"),
						row.getDouble("aircraft_groundspeed_kt_Y_at_fuel_start"),

						row.getDouble("aircraft_groundspeed_kt_X_at_fuel_end"),
						row.getDouble("aircraft_groundspeed_kt_Y_at_fuel_end"),

						//=====================================================
						// track angle degrees
						row.getDouble("aircraft_track_angle_deg_at_fuel_start"),
						row.getDouble("aircraft_track_angle_deg_at_fuel_end"),

						// track angle radians
						row.getDouble("aircraft_track_angle_rad_at_fuel_start"),
						row.getDouble("aircraft_track_angle_rad_at_fuel_end"),

						// =======================================================
						// computed vertical rate between fuel start and fuel stop
						row.getDouble("aircraft_computed_vertical_rate_ft_min"),

						// vertical rate
						row.getDouble("aircraft_vertical_rate_ft_min_at_fuel_start"),
						row.getDouble("aircraft_vertical_rate_ft_min_at_fuel_end"),

						//===================== speeds ================
						// mach 
						row.getDouble("aircraft_mach_at_fuel_start"),
						row.getDouble("aircraft_mach_at_fuel_end"),
						// TAS
						row.getDouble("aircraft_TAS_at_fuel_start"),
						row.getDouble("aircraft_TAS_at_fuel_end"),
						// CAS
						row.getDouble("aircraft_CAS_at_fuel_start"),
						row.getDouble("aircraft_CAS_at_fuel_end"),

						//========= airport elevations ================
						// airports departure and arrival elevation feet
						row.getFloat("origin_elevation_feet"),
						row.getFloat("destination_elevation_feet"),

						// flight distance and flight duration
						row.getDouble("flight_distance_Nm"),
						row.getLong("flight_duration_sec"),

						// difference relative to take-off
						row.getLong("fuel_burnt_start_relative_to_takeoff_sec"),
						row.getLong("fuel_burnt_end_relative_to_takeoff_sec"),
						row.getLong("fuel_burnt_start_relative_to_landed_sec"),
						row.getLong("fuel_burnt_end_relative_to_landed_sec"),

						// aircraft data
						// 7th November 2025 - added to differenciate aircraft with or without winglets or sharklets
						row.getString("aircraft_ICAO_Code") , 

						row.getInt("Num_Engines"),

						row.getFloat("Approach_Speed_knot"),
						row.getFloat("Length_ft"),

						// key discriminant for fuel efficiency
						row.getFloat("Wingspan_ft_without_winglets_sharklets"),
						row.getFloat("Wingspan_ft_with_winglets_sharklets"),

						row.getFloat("Tail_Height_at_OEW_ft"),
						row.getFloat("Wheelbase_ft"),
						row.getFloat("Cockpit_to_Main_Gear_ft"),
						row.getFloat("Main_Gear_Width_ft"),

						// these data types are consistent with those use in the aircrafts data reader
						row.getDouble("MTOW_kg"),
						row.getDouble("MALW_kg"),

						row.getFloat("Parking_Area_ft2"),

						// 26th October 2025
						row.getInt("flight_date_year"),
						row.getInt("flight_date_month"),
						row.getInt("flight_date_day_of_the_year")

						) ;
				fuelRecords.add(record);
			});
			// write parquet file
			FileSystemOutputFile outputFile = new FileSystemOutputFile(file);
			try (CarpetWriter<FuelExtendedDataRecord> writer = new CarpetWriter<>(outputFile, FuelExtendedDataRecord.class)) {
				for (FuelExtendedDataRecord fuelRecord : fuelRecords) {
					writer.write(fuelRecord);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		logger.info("Parquet file <<" + file.getAbsolutePath() + ">> written successfully!");
	}

	/**
	 * capable of reading both the training fuel parquet or the ranking fuek parquet files
	 * 27th December 2025 - flight list data table is filtered by aircraft ICAO type code
	 * @param flightListDataTable 
	 * @param train_rank_value
	 * @throws IOException
	 */
	public void readParquet(final FlightListData flightListData ) throws IOException {

		logger.info("--- fuel data for = <<" + this.getTrain_rank_value() +">> ---" );
		this.createEmptyFuelDataTable();

		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();

			File fuelFile = folderDiscovery.getFuelFileFromFileName(this.getTrain_rank_value());
			File flightListFile = folderDiscovery.getFlightListFileFromFileName(this.getTrain_rank_value());

			if ( (fuelFile != null) && fuelFile.exists() && fuelFile.isFile() && (flightListFile != null) && flightListFile.exists() && flightListFile.isFile()  ) {

				logger.info("file = " + fuelFile.getAbsolutePath() + " found !!!");
				var reader = new CarpetReader<>(fuelFile, FuelDataRecord.class);
				Iterator<FuelDataRecord> iterator = ((CarpetReader<FuelDataRecord>) reader).iterator();
				int count = 0;

				while (iterator.hasNext()) {
					FuelDataRecord record = iterator.next();
					//System.out.println(record);
					if ( flightListData.getListOfUniqueFlightIds().contains( record.flight_id() ) ) {
						this.appendRowToFuelDataTable(record);
						if (count > 10) {
							//break;
						}
					}
					count = count + 1;
				}
				logger.info(this.fuelDataTable.print(10));

			} else {
				logger.info("Error -> file not found -> in repo -> " + this.getTrain_rank_value() );
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		logger.info("Parquet file <<" + this.getTrain_rank_value() + ">> Fuel read successfully!");
	}
}
