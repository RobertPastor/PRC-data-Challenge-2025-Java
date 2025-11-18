package flights;

import tech.tablesaw.api.*;
import tech.tablesaw.selection.Selection;
import utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.io.FileSystemOutputFile;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flights.FlightDataSchema.FlightDataRecord;
import folderDiscovery.FolderDiscovery;

public class FlightDataTable extends Table {

	private static final Logger logger = Logger.getLogger(FlightDataTable.class.getName());

	protected train_rank_final train_rank_value;

	protected String flight_id = "";
	public Table flightDataTable = null;

	public FlightDataTable( final Table table) {
		super("Dummy Flight Data");
		this.setFlightDataTable(table);
	}

	// constructor
	protected FlightDataTable(final train_rank_final train_rank_value , final String flight_id) {
		super("Flight Data");
		this.setFlight_id(flight_id);
		this.setTrain_rank_value(train_rank_value);
		logger.info("create flight Data table");
	}

	public void createEmptyFlightDataTable( ) {
		this.flightDataTable = Table.create("Flight Data",
				StringColumn.create("flight_id"),
				InstantColumn.create("timestamp"),

				DoubleColumn.create("longitude"),
				DoubleColumn.create("latitude"),

				DoubleColumn.create("altitude"),
				DoubleColumn.create("groundspeed"),

				DoubleColumn.create("track"),

				DoubleColumn.create("vertical_rate"),
				DoubleColumn.create("mach"),

				StringColumn.create("typecode"),

				DoubleColumn.create("TAS"),
				DoubleColumn.create("CAS"),
				StringColumn.create("source"));

	}
	/**
	 * check if flight with flidht_id is available in the flight data
	 * @param flight_id
	 * @return
	 */
	public boolean  flightIdIsExisting( final String flight_id ) {

		//InstantColumn timestampColumn = this.getFlightDataTable().instantColumn("timestamp");
		StringColumn flightIdColumn = this.getFlightDataTable().stringColumn("flight_id");

		Table filtered = this.getFlightDataTable().where(flightIdColumn.isEqualTo(flight_id));
		//System.out.println( filtered.shape() );

		return ( filtered.rowCount() > 0);
	}

	public void appendRowToFlightDataTable(  FlightDataRecord record ) {

		Row row = this.flightDataTable.appendRow();
		row.setString("flight_id", record.flight_id());
		row.setInstant("timestamp", record.timestamp());

		row.setDouble("longitude", record.longitude());
		row.setDouble("latitude", record.latitude());

		row.setDouble("altitude", record.altitude());

		row.setDouble("groundspeed", record.groundspeed());
		row.setDouble("vertical_rate", record.vertical_rate());
		row.setDouble("mach", record.mach());

		row.setString("typecode", record.typecode());

		row.setDouble("TAS", record.TAS());
		row.setDouble("CAS", record.CAS());

		row.setString("source", record.source());
	}

	/**
	 * do not provide latitude and longitudes to the model
	 * instead convert latitude and longitude using sine and cosine
	 * 
	 * warning range of latitude is Latitude:
	 * Values range from -90° to +90°.
	 * Lines of latitude run east-west and are parallel to each other.
	 * The equator is at 0° latitude.
	 * 
	 * Longitude:
	 * Values range from -180° to +180°.
	 * Lines of longitude run north-south and converge at the poles.
	 * The prime meridian (0° longitude) runs through Greenwich, England.
	 * These ranges define the geographic coordinate system used to specify locations on Earth.
	 * 
	 * same function to compute latitude and longitude of airports location
	 */
	public void extendWithLatitudeLongitudeCosineSine( ) {

		DoubleColumn latitude_cosine_column = DoubleColumn.create("latitude_cosine");
		this.flightDataTable.addColumns(latitude_cosine_column);

		DoubleColumn latitude_sine_column = DoubleColumn.create("latitude_sine");
		this.flightDataTable.addColumns(latitude_sine_column);

		DoubleColumn longitude_cosine_column = DoubleColumn.create("longitude_cosine");
		this.flightDataTable.addColumns(longitude_cosine_column);

		DoubleColumn longitude_sine_column = DoubleColumn.create("longitude_sine");
		this.flightDataTable.addColumns(longitude_sine_column);

		//System.out.println( this.flightDataTable.structure() );

		Iterator<Row> iter = this.flightDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();

			double origin_latitude_degrees = row.getDouble("latitude");
			double origin_longitude_degrees = row.getDouble("longitude");
			//@TODO
			// need to change value range to 0.0 360.0 for cosinus and sinus to work without discontinuity
			row.setDouble ("latitude_cosine" , Math.cos(Math.toRadians(origin_latitude_degrees)));
			row.setDouble("latitude_sine" , Math.sin(Math.toRadians(origin_latitude_degrees)));

			row.setDouble("longitude_cosine" , Math.cos(Math.toRadians(origin_longitude_degrees)));
			row.setDouble("longitude_sine" , Math.sin(Math.toRadians(origin_longitude_degrees)));
		}
	}

	/**
	 * retrieves a list of distinct (unique) aircraft ICAO codes from a train, rank or final flight list
	 * @return
	 */
	public SortedSet<String> getListOfUniqueAircraftICAOTypes() {

		SortedSet<String> listAircrafICAOcodes = new TreeSet<String>();
		Iterator<Row> iter = this.flightDataTable.iterator();
		while ( iter.hasNext()) {
			Row row = iter.next();
			String aircraftICAOcode = row.getString("typecode");
			listAircrafICAOcodes.add(aircraftICAOcode);
		}
		return listAircrafICAOcodes;
	}

	public train_rank_final getTrain_rank_value() {
		return train_rank_value;
	}

	public void setTrain_rank_value(train_rank_final train_rank_value) {
		this.train_rank_value = train_rank_value;
	}

	public void setFlight_id(String flight_id) {
		this.flight_id = flight_id;
	}

	public void setFlightDataTable(Table flightDataTable) {
		this.flightDataTable = flightDataTable;
	}

	public String getFlight_id() {
		return flight_id;
	}

	public Table getFlightDataTable() {
		return this.flightDataTable;
	}

	public void generateParquetFileFor(final train_rank_final train_rank_final_value , final String flight_id) throws IOException {

		logger.info("--- start write parquet <<" + train_rank_final_value + ">> parquet file <<" +  flight_id + ">>  ------");

		String currentDateTimeAsStr = Utils.getCurrentDateTimeasStr();
		String fileName  = flight_id + "_" + train_rank_final_value + "_" + "interpolated" + "_" + currentDateTimeAsStr + "_" + ".parquet";

		String folderStr = FolderDiscovery.getTrainRankFinalInterpolatedFlightsOutputfolderStr(this.getTrain_rank_value());

		Path path = Paths.get(folderStr , fileName );
		File file = path.toFile();

		logger.info(fileName);
		logger.info(file.getAbsolutePath());

		try {
			List<FlightDataRecord> flightRecords = new ArrayList<FlightDataRecord>();
			this.getFlightDataTable().stream().forEach(row -> {
				FlightDataRecord record = new FlightDataRecord(

						row.getString("flight_id"),
						row.getInstant("timestamp"),

						row.getString("typecode") , 
						row.getString("source") , 

						row.getDouble("longitude"),
						row.getDouble("latitude"),
						row.getDouble("altitude"),

						row.getDouble("groundspeed"),
						row.getDouble("track"),
						row.getDouble("vertical_rate"),
						row.getDouble("mach"),
						row.getDouble("TAS"),
						row.getDouble("CAS")
						) ;

				flightRecords.add(record);

			});;
			// write parquet
			FileSystemOutputFile outputFile = new FileSystemOutputFile(file);
			try (CarpetWriter<FlightDataRecord> writer = new CarpetWriter<>(outputFile, FlightDataRecord.class)) {
				for (FlightDataRecord flightRecord : flightRecords) {
					writer.write(flightRecord);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		logger.info("Parquet file <<" + file.getAbsolutePath() + ">> written successfully!");
	}

}
