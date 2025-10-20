package airports;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.jerolba.carpet.CarpetReader;

import airports.AirportsDataSchema.AirportDataRecord;
import folderDiscovery.FolderDiscovery;

public class AirportsData extends AirportsDataTable {

	public void readParquet() throws IOException {
		this.createEmptyAirportsDataTable();
		File file = null;
		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();
			
			file = folderDiscovery.getAirportsFileFromFileName();
			if (file != null) {
				var reader = new CarpetReader<>(file, AirportDataRecord.class);
				Iterator<AirportDataRecord> iterator = ((CarpetReader<AirportDataRecord>) reader).iterator();
				int count = 0;
				
				while (iterator.hasNext()) {
					AirportDataRecord r = iterator.next();
				    //System.out.println(r);
				    
				    this.appendRowToAirportsDataTable(r);
				    if (count > 10) {
				    	break;
				    }
				    count = count + 1;
				}
				System.out.println(this.airportsDataTable.print(10));
			}
			
		} catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
        System.out.println("Parquet file <<" + file.getName() + ">> read successfully!");
		
	}
	
	public void writeParquet( )throws IOException {
	
	}
}
