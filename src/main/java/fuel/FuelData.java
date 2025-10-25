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

import dataChallengeEnums.DataChallengeEnums.train_rank;
import folderDiscovery.FolderDiscovery;
import fuel.FuelDataSchema.FuelDataRecord;
import fuel.FuelDataSchema.FuelExtendedDataRecord;
import utils.Utils;

public class FuelData extends FuelDataTable {

	private static final Logger logger = Logger.getLogger(FuelData.class.getName());

	public FuelData( train_rank value ) {
		super(value);
	}
	
	public void generateParquetFileFor( ) throws IOException {
		
		String currentDateTimeAsStr = Utils.getCurrentDateTimeasStr();
		String folderStr = "C:/Users/rober/eclipse-2025-09/eclipse-jee-2025-09-R-win32-x86_64/Data-Challenge-2025/documents";
		String fileName  = "ExtendedFuel_" + this.getTrain_rank_value() + "_" + currentDateTimeAsStr + ".parquet";
		
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
	        				
	        				row.getDouble("time_diff_seconds") , 
	        				row.getFloat("fuel_flow_kg_sec") ,
	        				
	        				// informations taken from the flight data between fuel start and fuel end
	        				row.getDouble("aircraft_latitude_at_fuel_start"),
	        				row.getDouble("aircraft_longitude_at_fuel_start"),
	        				
	        				row.getDouble("aircraft_latitude_at_fuel_end"),
	        				row.getDouble("aircraft_longitude_at_fuel_end"),
	        				
	        				row.getDouble("aircraft_distance_flown_Nm"),
	        				
	        				row.getFloat("aircraft_altitude_ft_at_fuel_start"),
	        				row.getFloat("aircraft_altitude_ft_at_fuel_end"),
	        				
	        				row.getFloat("aircraft_computed_vertical_rate_ft_min"),
	        				
	        				row.getFloat("aircraft_groundspeed_kt_at_fuel_start"),
	        				row.getFloat("aircraft_groundspeed_kt_at_fuel_end"),
	        				
	        				row.getFloat("aircraft_track_angle_deg_at_fuel_start"),
	        				row.getFloat("aircraft_track_angle_deg_at_fuel_end"),
	        				
	        				row.getFloat("aircraft_vertical_rate_ft_min_at_fuel_start"),
	        				row.getFloat("aircraft_vertical_rate_ft_min_at_fuel_end"),
	        				
	        				// airports departure and arrival elevation feet
	        				row.getFloat("origin_elevation_feet"),
	        				row.getFloat("destination_elevation_feet"),
	        				
	        				// flight distance and flight duration
	        				row.getDouble("flight_distance_Nm"),
	        				row.getLong("flight_duration_sec")
	        				
	        				) ;
	        	
	        		fuelRecords.add(record);
	        		
	        	});;
		      
	        	FileSystemOutputFile outputFile = new FileSystemOutputFile(file);
		        try (CarpetWriter<FuelExtendedDataRecord> writer = new CarpetWriter<>(outputFile, FuelExtendedDataRecord.class)) {
		        	for (FuelExtendedDataRecord fuelRecord : fuelRecords) {
		                writer.write(fuelRecord);
		            }
		        }
	        
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
        
        System.out.println("Parquet file <<" + file.getAbsolutePath() + ">> written successfully!");
	 }

	/**
	 * capable of reading both the training fuel parquet or the ranking fuek parquet
	 * @param train_rank_value
	 * @throws IOException
	 */
	public void readParquet( ) throws IOException {
		
		logger.info("--- fuel data for = <<" + this.getTrain_rank_value() +">>" );
		this.createEmptyFuelDataTable();

		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();

			File file = folderDiscovery.getFuelFileFromFileName(this.getTrain_rank_value());
			if ( (file != null) && file.exists() && file.isFile()) {

				logger.info("file = " + file.getAbsolutePath() + " found !!!");
				var reader = new CarpetReader<>(file, FuelDataRecord.class);
				Iterator<FuelDataRecord> iterator = ((CarpetReader<FuelDataRecord>) reader).iterator();
				int count = 0;

				while (iterator.hasNext()) {
					FuelDataRecord r = iterator.next();
					//System.out.println(r);

					this.appendRowToFuelDataTable(r);

					if (count > 10) {
						//break;
					}
					count = count + 1;
				}
				System.out.println(this.fuelDataTable.print(10));
				
			} else {
				System.out.println("Error -> file not found -> in repo -> " + this.getTrain_rank_value() );
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		System.out.println("Parquet file <<" + this.getTrain_rank_value() + ">> Fuel read successfully!");
	}
}
