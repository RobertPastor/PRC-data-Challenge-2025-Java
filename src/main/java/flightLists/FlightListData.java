package flightLists;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import com.jerolba.carpet.CarpetReader;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListDataSchema.FlightListDataRecord;
import folderDiscovery.FolderDiscovery;

public class FlightListData extends FlightListDataTable {

	private static final Logger logger = Logger.getLogger(FlightListData.class.getName());
	private String aircraft_type_code = "";

	public FlightListData( train_rank_final value, String aircraft_type_code ) {
		this.train_rank_value = value;
		this.aircraft_type_code = aircraft_type_code;
	}
	
	public FlightListData( train_rank_final value ) {
		this.train_rank_value = value;
		this.aircraft_type_code = "A320";
	}

	/**
	 * read the flight list from a parquet file
	 * 27th December 2025 - filter on one aircraft type code
	 * @throws IOException
	 */
	public void readParquet( ) throws IOException {
		
		logger.info("----- <<" + this.getTrain_rank_value() + ">> ------");
		this.createEmptyFlightListDataTable();
		File file = null;
		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();
			file = folderDiscovery.getFlightListFileFromFileName(this.getTrain_rank_value() );
			logger.info(file.getAbsolutePath());
			
			var reader = new CarpetReader<>(file, FlightListDataRecord.class);
			Iterator<FlightListDataRecord> iterator = ((CarpetReader<FlightListDataRecord>) reader).iterator();

			while (iterator.hasNext()) {
				FlightListDataSchema.FlightListDataRecord record = iterator.next();
				//System.out.println(r);
				
				if ( record.aircraft_type().equalsIgnoreCase(aircraft_type_code)) {

					// assert - sanity check
					assert (record.takeoff() != null ) && (record.takeoff().getEpochSecond() > 0);
					assert (record.landed() != null) && (record.landed().getEpochSecond() > 0);
					assert record.landed().isAfter(record.takeoff()) ;
					
					this.appendRowToFlightListDataTable(record);
				}
			}
			//logger.info( this.flightListDataTable.shape() );
			//logger.info( this.flightListDataTable.structure().print() );
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		logger.info("Parquet file <<" + this.getTrain_rank_value() + ">> Flight List <<" + file.getAbsolutePath() +">> read successfully!");
	}
}
