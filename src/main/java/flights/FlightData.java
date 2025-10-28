package flights;

import java.io.File;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flights.FlightDataSchema.FlightDataRecord;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.variant.VariantBuilder;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.impl.read.VariantConverter;
import com.jerolba.carpet.impl.read.converter.VariantRead;

import aircrafts.AircraftsData;
import folderDiscovery.FolderDiscovery;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;


class CustomException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1346311432020834637L;

	public CustomException(String message) {
		super(message);
	}
}


public class FlightData extends FlightDataTable {

	private static final Logger logger = Logger.getLogger(FlightDataTable.class.getName());
	
	// map of interpolation function
	private Map<String, PolynomialSplineFunction> interpolationFunctionMap = null;


	public Map<String, PolynomialSplineFunction> getInterpolationFunctionMap() {
		return interpolationFunctionMap;
	}

	public FlightData( train_rank train_rank_value , final String flight_id ) {
		super(train_rank_value , flight_id);
		interpolationFunctionMap = new HashMap<>();
	}

	/**
		 idx  | time_diff_seconds |   fuel_flow_kg_sec |   origin_longitude |   origin_latitude |   origin_elevation |   destination_longitude |   destination_latitude |   destination_elevation |   flight_distance_Nm |   flight_duration_sec |   MTOW_lb |   MALW_lb |   Num_Engines |   Approach_Speed_knot |   Wingspan_ft_without_winglets_sharklets |   Length_ft |   Parking_Area_ft2 |   fuel_burn_relative_start |   fuel_burn_relative_end |   latitude |   longitude |   altitude |   groundspeed |   track |   vertical_rate |   mach |   TAS |   CAS |   timestamp_relative_start |
		 +====+===================+====================+====================+===================+====================+=========================+========================+=========================+======================+=======================+===========+===========+===============+=======================+==========================================+=============+====================+============================+==========================+============+=============+============+===============+=========+=================+========+=======+=======+============================+
		 |  0 |       1800.04     |           1.38886  |            101.71  |           2.74558 |                 69 |                 4.76389 |                52.3086 |                     -11 |              5533.53 |                 44297 |    560000 |    425000 |             2 |                   144 |                                    197.3 |       206.1 |            43747   |                    36929.4 |                  38729.5 |    45.1833 |    24.35    |    35974.9 |       467     | 302.324 |        0        |   0.86 |     0 |     0 |                    36929.4 |
		 +---
	 */
	
	public void generatedInterpolationFunction ( final String columnNameToInterpolate ) {
		
		// filter the table with a selection
		Table filteredTable = this.getFlightDataTable().where(this.getFlightDataTable().doubleColumn(columnNameToInterpolate).isNotMissing());
		
		// create a list of instants as X values in the interpolation
		Instant[] xInstants =  (Instant[]) filteredTable.column("timestamp").asObjectArray();
		
		int sizeOfArray = xInstants.length;
		double[] xSeconds = new double[sizeOfArray];
		
		double[] yValues = new double[sizeOfArray];
		DoubleColumn c = (DoubleColumn) filteredTable.column(columnNameToInterpolate);
		yValues = c.asDoubleArray();
				
		// convert Instant to seconds (long)

        for ( int i = 0 ; i < xInstants.length ; i++) {
              xSeconds[i] = xInstants[i].getEpochSecond();
        }
		// Perform interpolation

        LinearInterpolator interpolator = new LinearInterpolator();
        PolynomialSplineFunction function = interpolator.interpolate(xSeconds, yValues);

        interpolationFunctionMap.put(columnNameToInterpolate, function);
	}
	  


	/**
	 * new method of reading parquet files and managing missing values (holes)
	 * @throws IOException
	 */
	public void readParquetWithStream() throws IOException {

		logger.info("----------- start read parquet with stream ------");

		this.createEmptyFlightDataTable();

		FolderDiscovery folderDiscovery = new FolderDiscovery();

		String fileName = this.getFlight_id() + ".parquet";
		try {

			File parquetFile = folderDiscovery.getFlightFileFromFileName(this.getTrain_rank_value() , fileName);
			//VariantBuilder variantBbuilder = new VariantBuilder();

			CarpetReader<FlightDataRecord> reader = new CarpetReader<FlightDataRecord>(parquetFile, FlightDataRecord.class);
			reader.stream().forEachOrdered(record -> {

				Row row = this.flightDataTable.appendRow();

				row.setString("flight_id", record.flight_id());
				row.setInstant("timestamp", record.timestamp());

				// assumption no holes in latitude nor in longitude
				if (record.latitude() == null) {
					//System.out.println("row = " + record.timestamp() + " -> longitude --> null found");
					//System.out.println("--- do nothing - do not fill empty cell ---");
				} else {
					row.setDouble("latitude" , record.latitude());
				}

				if (record.longitude() == null) {
					//System.out.println("row = " + record.timestamp() + " -> longitude --> null found");
					//System.out.println("--- do nothing - do not fill empty cell ---");
				} else {
					row.setDouble("longitude", record.longitude());
				}
				/**
				 * managing holes in the floating values of altitude
				 */
				if ( record.altitude() == null) {
					//System.out.println("row = " + record.timestamp() + " -> altitude --> null found");
					//System.out.println("--- do nothing - do not fill empty cell ---");
				} else {
					row.setDouble("altitude", record.altitude());
				}

				if ( record.groundspeed() == null) {
					//System.out.println("row = " + record.timestamp() + " -> groundspeed --> null found");
					//System.out.println("--- do nothing - do not fill empty cell ---");
				} else {
					row.setDouble("groundspeed", record.groundspeed());
				}

				if ( record.track() == null) {
					//System.out.println("row = " + record.timestamp() + " -> track --> null found");
					//System.out.println("--- do nothing - do not fill empty cell ---");
				} else {
					row.setDouble("track", record.track());
				}

				if ( record.vertical_rate() == null) {
					//System.out.println("row = " + record.timestamp() + " -> vertical_rate --> null found");
					//System.out.println("--- do nothing - do not fill empty cell ---");
				} else {
					row.setDouble("vertical_rate", record.vertical_rate());
				}

				if ( record.mach() == null) {
					//System.out.println("row = " + record.timestamp() + " -> mach --> null found");
					//System.out.println("--- do nothing - do not fill empty cell ---");
				} else {
					row.setDouble("mach", record.mach());
				}

				if ( record.TAS() == null) {
					//System.out.println("row = " + record.timestamp() + " -> TAS --> null found");
					//System.out.println("--- do nothing - do not fill empty cell ---");
				} else {
					row.setDouble("TAS", record.TAS());
				}

				if ( record.CAS() == null) {
					//System.out.println("row = " + record.timestamp() + " -> CAS --> null found");
					//System.out.println("--- do nothing - do not ill empty cell ---");
				} else {
					row.setDouble("CAS", record.CAS());
				}
			});
			// interpolate
			//interpolateFlightDataMissingColumnsValues();
			List<String> columnsToInterpolateList = Arrays.asList("altitude", "latitude" , "longitude");
			for ( String columnName : columnsToInterpolateList) {
				generatedInterpolationFunction( columnName );
			}

			//System.out.println( this.getFlightDataTable().print(10));

		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * interpolate using the basic TableSaw interpolator 
	 * that is only filling with the nearest non null value
	 * @param columnNameToInterpolate
	 */
	public void interpolateDoubleColumnsFromMissingRecords ( final String columnNameToInterpolate ) {

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
	/**
	 * not used
	 * @throws IOException
	 * @throws CustomException 
	 */
	public void readParquetWithNulls() throws IOException, CustomException {

		throw new CustomException("do not use this one, use the read Parquet with stream");

		logger.info("----------- start read parquet with nulls ------");

		this.createEmptyFlightDataTable();
		FolderDiscovery folderDiscovery = new FolderDiscovery();

		String fileName = this.getFlight_id() + ".parquet";
		try {

			File parquetFile = folderDiscovery.getFlightFileFromFileName(this.getTrain_rank_value() , fileName);
			//VariantBuilder variantBbuilder = new VariantBuilder();

			try ( var reader = new CarpetReader.Builder<>(parquetFile, FlightDataRecord.class)
					.failOnNullForPrimitives(false)
					.buildParquetReader()) {

				FlightDataRecord record;		
				while ( ( record = reader.read() ) != null ) {
					//System.out.println( record.altitude().getClass().getCanonicalName() );
					System.out.println( record.timestamp());
				}
				reader.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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