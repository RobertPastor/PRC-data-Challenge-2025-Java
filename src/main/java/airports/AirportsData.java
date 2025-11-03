package airports;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import com.jerolba.carpet.CarpetReader;

import airports.AirportsDataSchema.AirportDataRecord;
import folderDiscovery.FolderDiscovery;

public class AirportsData extends AirportsDataTable {
	
	private static final Logger logger = Logger.getLogger(AirportsData.class.getName());

	public void readParquet() throws IOException {
		this.createEmptyAirportsDataTable();
		File file = null;
		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();
			
			file = folderDiscovery.getAirportsFileFromFileName();
			logger.info(file.getAbsolutePath());
			
			if ( (file != null) && file.exists() && file.isFile() ) {
				
				var reader = new CarpetReader<>(file, AirportDataRecord.class);
				Iterator<AirportDataRecord> iterator = ((CarpetReader<AirportDataRecord>) reader).iterator();
				int count = 0;
				
				while (iterator.hasNext()) {
					AirportDataRecord record = iterator.next();
				    //System.out.println(r);
				    
				    this.appendRowToAirportsDataTable(record);
				    if (count > 10) {
				    	//break;
				    }
				    count = count + 1;
				}
				logger.info(this.airportsDataTable.shape());
				logger.info(this.airportsDataTable.print(10));
			}
			
		} catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
		logger.info("Parquet file <<" + file.getName() + ">> read successfully!");
	}
}
