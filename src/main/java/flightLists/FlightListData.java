package flightLists;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import com.jerolba.carpet.CarpetReader;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListDataSchema.FlightListDataRecord;
import folderDiscovery.FolderDiscovery;
import fuel.FuelData;

public class FlightListData extends FlightListDataTable {

	private static final Logger logger = Logger.getLogger(FlightListData.class.getName());

	public FlightListData( train_rank value ) {
		this.train_rank_value = value;
	}

	public void readParquet( ) throws IOException {
		
		logger.info("--- constructor ---");

		this.createEmptyFlightListDataTable();
		File file = null;
		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();
			
			file = folderDiscovery.getFlightListFileFromFileName(this.getTrain_rank_value() );
			var reader = new CarpetReader<>(file, FlightListDataRecord.class);
			Iterator<FlightListDataRecord> iterator = ((CarpetReader<FlightListDataRecord>) reader).iterator();
			int count = 0;

			while (iterator.hasNext()) {
				FlightListDataSchema.FlightListDataRecord record = iterator.next();
				//System.out.println(r);

				// assert - sanity check
				assert (record.takeoff() != null ) && (record.takeoff().getEpochSecond() > 0);
				assert (record.landed() != null) && (record.landed().getEpochSecond() > 0);
				assert record.landed().isAfter(record.takeoff()) ;
				
				this.appendRowToFlightListDataTable(record);

				if (count > 10) {
					//break;
				}
				count = count + 1;
			}
			//System.out.println(this.flightListDataTable.print(10));
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		System.out.println("Parquet file <<" + this.getTrain_rank_value() + ">> Flight List <<" + file.getAbsolutePath() +">> read successfully!");

	}
}
