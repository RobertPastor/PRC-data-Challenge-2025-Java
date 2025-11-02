package airportsLegacyExcel;

import java.util.logging.Logger;

import airportsLegacyExcel.AirportsLegacyExcelDataSchema.AirportDataRecord;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

public class AirportsLegacyExcelDataTable extends Table {

	private static final Logger logger = Logger.getLogger(AirportsLegacyExcelDataTable.class.getName());

	protected Table airportsDataTable = null;
	
	protected AirportsLegacyExcelDataTable() {
		super("Airports Legacy EXCEL Data");
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
				
				FloatColumn.create("latitude degrees"),
				FloatColumn.create("longitude degrees"),
				FloatColumn.create("elevation meters"));
	}
	
	public void appendRowToAirportsDataTable(  AirportDataRecord record ) {

		Row row = this.airportsDataTable.appendRow();

		row.setInt("Id", record.Id());
		
		row.setString("Name", record.Name());
		row.setString("Airport short name", record.Airport_Short_Name());
		row.setString("Country", record.Country());
		row.setString("IATA", record.IATA());
		row.setString("ICAO", record.ICAO());
		
		row.setDouble("latitude degrees", record.airport_latitude_degrees());
		row.setDouble("longitude degrees", record.airport_longitude_degrees());
		
		row.setFloat("elevation meters", record.airport_elevation_meters());
	}
}
