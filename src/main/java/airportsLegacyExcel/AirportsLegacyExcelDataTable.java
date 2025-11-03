package airportsLegacyExcel;

import java.util.logging.Logger;

import airportsLegacyExcel.AirportsLegacyExcelDataSchema.AirportDataRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

public class AirportsLegacyExcelDataTable extends Table implements Runnable {

	private static final Logger logger = Logger.getLogger(AirportsLegacyExcelDataTable.class.getName());

	protected Table airportsDataTable = null;
	protected static String className = AirportsLegacyExcelDataTable.class.getName();
	
	public Table getAirportsDataTable() {
		return this.airportsDataTable;
	}

	protected AirportsLegacyExcelDataTable() {
		super("Airports Legacy EXCEL Data");
		logger.info("constructor");
	}
	
	public void createEmptyAirportsDataTable() {
		
		logger.info("create empty airports legacy table from EXCEL");
		
		//"Id","Name","Airport short name","Country","IATA","ICAO","latitude degrees","longitude degrees","elevation meters");

		this.airportsDataTable = Table.create("Aircrafts Legacy EXCEL Data",
				
				IntColumn.create("Id"),

				StringColumn.create("Name") ,
				StringColumn.create("Airport short name") ,
				StringColumn.create("Country") ,
				StringColumn.create("IATA") ,
				StringColumn.create("ICAO") ,
				
				DoubleColumn.create("airport latitude degrees"),
				DoubleColumn.create("airport longitude degrees"),
				FloatColumn.create("airport elevation meters"));
	}
	
	public void appendRowToAirportsDataTable(  AirportDataRecord record ) {
		
		logger.info("append row to airports legacy table from EXCEL");
		Row row = this.airportsDataTable.appendRow();

		row.setInt("Id", record.Id());
		
		row.setString("Name", record.Name());
		row.setString("Airport short name", record.Airport_Short_Name());
		row.setString("Country", record.Country());
		row.setString("IATA", record.IATA());
		row.setString("ICAO", record.ICAO());
		
		row.setDouble("airport latitude degrees", record.airport_latitude_degrees());
		row.setDouble("airport longitude degrees", record.airport_longitude_degrees());
		
		row.setFloat("airport elevation meters", record.airport_elevation_meters());
	}
	
	public void createLatitudeLongitudeAsRadiansColumns ()		 {
		// destination airport latitude and longitude in radians
	
		DoubleColumn airport_latitude_rad_column = DoubleColumn.create("airport latitude radians");
		this.airportsDataTable.addColumns(airport_latitude_rad_column);
	
		DoubleColumn airport_longitude_rad_column = DoubleColumn.create("airport longitude radians");
		this.airportsDataTable.addColumns(airport_longitude_rad_column);
	}
	
	public void extendOneRowWithLatitudeLongitudeRadians (Row row) {
		
		String airportICAOcode = row.getString("ICAO");
		int airportId = row.getInt("Id");
		System.out.println( className + " - extending row with Id = " + String.valueOf(airportId) + " for airport ICAO code = "+ airportICAOcode);
			
		// -------------- destination airport latitude longitude in radians (27/10/2025)
		double airport_latitude_degrees = row.getDouble("airport latitude degrees");
		double airport_longitude_degrees = row.getDouble("airport longitude degrees");
		
		row.setDouble("airport latitude radians" , Math.toRadians ( airport_latitude_degrees ) );
		row.setDouble("airport longitude radians" , Math.toRadians ( airport_longitude_degrees ) );

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
}
