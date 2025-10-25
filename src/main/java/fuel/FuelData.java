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
	
	public void generateParquetFileFor() throws IOException {
		
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
	        		FuelExtendedDataRecord record = new FuelExtendedDataRecord(row.getInt("idx"),
	        				row.getString("flight_id") , row.getInstant("start"), row.getInstant("end"));
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
        
        System.out.println("Parquet file written successfully!");
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
