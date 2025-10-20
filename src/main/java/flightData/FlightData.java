package flightData;

import java.io.File;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import java.io.IOException;
import java.util.Iterator;

import com.jerolba.carpet.CarpetReader;

import flightData.FlightDataSchema.FlightDataRecord;
import folderDiscovery.FolderDiscovery;

import tech.tablesaw.api.*; 

public class FlightData extends FlightDataTable {
	
	private String flight_id = "";
	
	public String getFlight_id() {
		return flight_id;
	}
	
	private train_rank train_rank_value;
	
	public train_rank getTrain_rank_value() {
		return train_rank_value;
	}
	
	FlightData( train_rank value , final String flight_id) {
		this.flight_id = flight_id;
		this.train_rank_value = value;
	}
	

	public void readParquet() throws IOException {
		this.createEmptyFlightDataTable();
		
		String fileName = this.getFlight_id() + ".parquet";
		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();
			
			File file = folderDiscovery.getFlightFileFromFileName(this.train_rank_value , fileName);
			var reader = new CarpetReader<>(file, FlightDataRecord.class);
			Iterator<FlightDataRecord> iterator = ((CarpetReader<FlightDataRecord>) reader).iterator();
			int count = 0;
			
			while (iterator.hasNext()) {
			    FlightDataSchema.FlightDataRecord r = iterator.next();
			    System.out.println(r);
			    
			    this.appendRowToFlightDataTable(r);
		        
			    if (count > 10) {
			    	break;
			    }
			    count = count + 1;
			}
			System.out.println(this.flightDataTable.print(10));
		} catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
        System.out.println("Parquet file <<" + fileName + ">> read successfully!");
		
	}
	
	public void writeParquet( )throws IOException {
	
	}

	

}