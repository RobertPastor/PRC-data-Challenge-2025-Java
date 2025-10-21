package genericCarpetParquetReader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.jerolba.carpet.CarpetReader;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import folderDiscovery.FolderDiscovery;
import genericCarpetParquetReader.FuelDataSchema.*;

public class ReadFromParquetUsingCarpetReader {

	static void readParquetFileFrom() throws IOException{
		
		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();
			// the name of the fuel file is hard coded in the Folder Discovery
			File file = folderDiscovery.getFuelFileFromFileName(train_rank.rank);
			var reader = new CarpetReader<>(file, FuelDataRecord.class);
			Iterator<FuelDataSchema.FuelDataRecord> iterator = ((CarpetReader<FuelDataSchema.FuelDataRecord>) reader).iterator();
			while (iterator.hasNext()) {
			    FuelDataSchema.FuelDataRecord r = iterator.next();
			    System.out.println(r);
			}

		} catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
        System.out.println("Parquet file read successfully!");
		
	}
}
