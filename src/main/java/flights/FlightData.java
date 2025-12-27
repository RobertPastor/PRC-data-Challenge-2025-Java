package flights;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.io.FileSystemOutputFile;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flights.FlightDataSchema.FlightDataRecord;
import folderDiscovery.FolderDiscovery;
import fuel.FuelDataSchema.FuelExtendedDataRecord;
import tech.tablesaw.api.Row;
import utils.CustomException;
import utils.Utils;


public class FlightData extends FlightDataTable {

	private static final Logger logger = Logger.getLogger(FlightData.class.getName());

	// constructor
	public FlightData( train_rank_final train_rank_value , final String flight_id ) {
		super(train_rank_value , flight_id);
	}

	/**
		 idx  | time_diff_seconds |   fuel_flow_kg_sec |   origin_longitude |   origin_latitude |   origin_elevation |   destination_longitude |   destination_latitude |   destination_elevation |   flight_distance_Nm |   flight_duration_sec |   MTOW_lb |   MALW_lb |   Num_Engines |   Approach_Speed_knot |   Wingspan_ft_without_winglets_sharklets |   Length_ft |   Parking_Area_ft2 |   fuel_burn_relative_start |   fuel_burn_relative_end |   latitude |   longitude |   altitude |   groundspeed |   track |   vertical_rate |   mach |   TAS |   CAS |   timestamp_relative_start |
		 +====+===================+====================+====================+===================+====================+=========================+========================+=========================+======================+=======================+===========+===========+===============+=======================+==========================================+=============+====================+============================+==========================+============+=============+============+===============+=========+=================+========+=======+=======+============================+
		 |  0 |       1800.04     |           1.38886  |            101.71  |           2.74558 |                 69 |                 4.76389 |                52.3086 |                     -11 |              5533.53 |                 44297 |    560000 |    425000 |             2 |                   144 |                                    197.3 |       206.1 |            43747   |                    36929.4 |                  38729.5 |    45.1833 |    24.35    |    35974.9 |       467     | 302.324 |        0        |   0.86 |     0 |     0 |                    36929.4 |
		 +---
	 */

	private void setMyOwnDouble( Row row , final String columnName , final Double doubleValueWithPotentialNull) {

		if ( (Double)doubleValueWithPotentialNull == null ) {
			row.setMissing(columnName);
		} else {
			row.setDouble(columnName, doubleValueWithPotentialNull);
		}
	}
	
	/**
	 * new method of reading parquet files and managing missing values (holes)
	 * @throws IOException
	 * @throws CustomException 
	 */
	public void readParquetWithStream() throws IOException, CustomException {

		logger.info("----------- start read <<" + this.getTrain_rank_value() + ">> parquet file <<" +  this.getFlight_id() + ">> using a stream ------");

		this.createEmptyFlightDataTable();
		FolderDiscovery folderDiscovery = new FolderDiscovery();

		String fileName = this.getFlight_id() + ".parquet";
		//logger.info(fileName);

		File parquetFile = folderDiscovery.getFlightFileFromFileName(this.getTrain_rank_value() , fileName);
		
		System.out.println("---------- > " + parquetFile.getAbsolutePath());
		System.out.println("---------- > " + parquetFile.getAbsolutePath());
		System.out.println("---------- > " + parquetFile.getAbsolutePath());
		
		if (( parquetFile != null ) &&  parquetFile.isFile() ) {

			try {
				CarpetReader<FlightDataRecord> reader = new CarpetReader<FlightDataRecord>(parquetFile, FlightDataRecord.class);
				Iterator<FlightDataRecord> iter = reader.iterator();
				while( iter.hasNext() ) {
				//.stream().forEachOrdered(record -> {
					FlightDataRecord flightDataRecord = iter.next();
					Row row = this.flightDataTable.appendRow();

					row.setString("flight_id", flightDataRecord.flight_id());
					row.setInstant("timestamp", flightDataRecord.timestamp());
					row.setString("typecode", flightDataRecord.typecode());
					row.setString("source", flightDataRecord.source());

					// assumption no holes in latitude nor in longitude
					if (flightDataRecord.latitude() == null) {
						//System.out.println("row = " + record.timestamp() + " -> longitude --> null found");
						//System.out.println("--- do nothing - do not fill empty cell ---");
					} else {
						this.setMyOwnDouble(row,"latitude" , flightDataRecord.latitude());
					}

					if (flightDataRecord.longitude() == null) {
						//System.out.println("row = " + record.timestamp() + " -> longitude --> null found");
						//System.out.println("--- do nothing - do not fill empty cell ---");
					} else {
						this.setMyOwnDouble(row,"longitude", flightDataRecord.longitude());
					}
					/**
					 * managing holes in the floating values of altitude
					 */
					if ( flightDataRecord.altitude() == null) {
						//System.out.println("row = " + record.timestamp() + " -> altitude --> null found");
						//System.out.println("--- do nothing - do not fill empty cell ---");
					} else {
						this.setMyOwnDouble(row, "altitude", flightDataRecord.altitude());
					}

					if ( flightDataRecord.groundspeed() == null) {
						//System.out.println("row = " + record.timestamp() + " -> groundspeed --> null found");
						//System.out.println("--- do nothing - do not fill empty cell ---");
					} else {
						this.setMyOwnDouble(row,"groundspeed", flightDataRecord.groundspeed());
					}

					if ( flightDataRecord.track() == null) {
						//System.out.println("row = " + record.timestamp() + " -> track --> null found");
						//System.out.println("--- do nothing - do not fill empty cell ---");
					} else {
						this.setMyOwnDouble(row,"track", flightDataRecord.track());
					}

					if ( flightDataRecord.vertical_rate() == null) {
						//System.out.println("row = " + record.timestamp() + " -> vertical_rate --> null found");
						//System.out.println("--- do nothing - do not fill empty cell ---");
					} else {
						this.setMyOwnDouble(row,"vertical_rate", flightDataRecord.vertical_rate());
					}

					if ( flightDataRecord.mach() == null) {
						//System.out.println("row = " + record.timestamp() + " -> mach --> null found");
						//System.out.println("--- do nothing - do not fill empty cell ---");
					} else {
						this.setMyOwnDouble(row,"mach", flightDataRecord.mach());
					}

					if ( flightDataRecord.TAS() == null) {
						//System.out.println("row = " + record.timestamp() + " -> TAS --> null found");
						//System.out.println("--- do nothing - do not fill empty cell ---");
					} else {
						this.setMyOwnDouble(row,"TAS", flightDataRecord.TAS());
					}

					if ( flightDataRecord.CAS() == null) {
						//System.out.println("row = " + record.timestamp() + " -> CAS --> null found");
						//System.out.println("--- do nothing - do not ill empty cell ---");
					} else {
						this.setMyOwnDouble(row,"CAS", flightDataRecord.CAS());
					}
				};
				//System.out.println( this.getFlightDataTable().print(10));

			}catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
		} else {
			throw new CustomException("file with name <<" + fileName + ">> ->> not found in -> <<" + this.train_rank_value + ">> database of flight files");
		}
	}


	/**
	 * interpolate using the basic TableSaw interpolator 
	 * that is only filling with the nearest non null value
	 * @param columnNameToInterpolate
	 */
	/**
	public void do_not_use_interpolateDoubleColumnsFromMissingRecords ( final String columnNameToInterpolate ) {

		final String interpolated_column_name = columnNameToInterpolate + "_" + "interpolated";

		DoubleColumn interpolated_column = DoubleColumn.create(interpolated_column_name);
		this.flightDataTable.addColumns(interpolated_column);

		// Fill interpolated column values with the source column values
		int sizeOfSourceColumn = this.getFlightDataTable().doubleColumn(columnNameToInterpolate).size();
		for (int i = 0; i < sizeOfSourceColumn ; i++) {
			interpolated_column.set(i, this.getFlightDataTable().doubleColumn(columnNameToInterpolate).get(i));
		}

		// backfill value extension
		Iterator<Double> iterBackFill = this.getFlightDataTable().doubleColumn(interpolated_column_name).interpolate().backfill().iterator() ;
		int i = 0;
		while ( iterBackFill.hasNext() ) {
			interpolated_column.set(i++, iterBackFill.next());
		}

		Iterator<Double> iterFrontFill = this.getFlightDataTable().doubleColumn(interpolated_column_name).interpolate().frontfill().iterator() ;
		i = 0;
		while ( iterFrontFill.hasNext() ) {
			interpolated_column.set(i++, iterFrontFill.next());
		}
	}
	 */
	/**
	 * not used
	 * @throws IOException
	 * @throws CustomException 
	 */



}