package flights;

import java.io.File;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flights.FlightDataSchema.FlightDataRecord;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.logging.Logger;

import com.jerolba.carpet.CarpetReader;

import aircrafts.AircraftsData;
import folderDiscovery.FolderDiscovery;
import java.util.logging.Logger;


public class FlightData extends FlightDataTable {
	
    private static final Logger logger = Logger.getLogger(FlightDataTable.class.getName());

	public FlightData( train_rank train_rank_value , final String flight_id ) {
		super(train_rank_value , flight_id);
	}
	
	public void extendFlightDataTable() {
		/**
		 idx  | time_diff_seconds |   fuel_flow_kg_sec |   origin_longitude |   origin_latitude |   origin_elevation |   destination_longitude |   destination_latitude |   destination_elevation |   flight_distance_Nm |   flight_duration_sec |   MTOW_lb |   MALW_lb |   Num_Engines |   Approach_Speed_knot |   Wingspan_ft_without_winglets_sharklets |   Length_ft |   Parking_Area_ft2 |   fuel_burn_relative_start |   fuel_burn_relative_end |   latitude |   longitude |   altitude |   groundspeed |   track |   vertical_rate |   mach |   TAS |   CAS |   timestamp_relative_start |
		 +====+===================+====================+====================+===================+====================+=========================+========================+=========================+======================+=======================+===========+===========+===============+=======================+==========================================+=============+====================+============================+==========================+============+=============+============+===============+=========+=================+========+=======+=======+============================+
		 |  0 |       1800.04     |           1.38886  |            101.71  |           2.74558 |                 69 |                 4.76389 |                52.3086 |                     -11 |              5533.53 |                 44297 |    560000 |    425000 |             2 |                   144 |                                    197.3 |       206.1 |            43747   |                    36929.4 |                  38729.5 |    45.1833 |    24.35    |    35974.9 |       467     | 302.324 |        0        |   0.86 |     0 |     0 |                    36929.4 |
		 +---
		 */
	}

	public void readParquet() throws IOException {
		
		this.createEmptyFlightDataTable();
		FolderDiscovery folderDiscovery = new FolderDiscovery();

		String fileName = this.getFlight_id() + ".parquet";
		try {
			
			File file = folderDiscovery.getFlightFileFromFileName(this.train_rank_value , fileName);
			var reader = new CarpetReader<>(file, FlightDataRecord.class);
			Iterator<FlightDataRecord> iterator = ((CarpetReader<FlightDataRecord>) reader).iterator();
			int count = 0;
			
			while (iterator.hasNext()) {
			    FlightDataSchema.FlightDataRecord r = iterator.next();
			    //System.out.println(r);
			    
			    this.appendRowToFlightDataTable(r);
			    if (count > 10) {
			    	//break;
			    }
			    count = count + 1;
			}
			//System.out.println(this.flightDataTable.print(10));
			///logger.info("Row count = " + this.flightDataTable.rowCount());

		} catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
        logger.info("Parquet file <<" + folderDiscovery.getFlightPath(this.train_rank_value , fileName) + ">> read successfully!");
	}
	
	public void writeParquet( )throws IOException {
	
	}

	

	

}