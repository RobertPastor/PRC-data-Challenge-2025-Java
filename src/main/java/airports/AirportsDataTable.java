package airports;

import airports.AirportsDataSchema.AirportDataRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;

public class AirportsDataTable  extends Table {

	public Table airportsDataTable = null;

	public Table getAirportsDataTable() {
		return airportsDataTable;
	}

	AirportsDataTable() {
		super("AirportsData");
	}

	public void createEmptyAirportsDataTable( ) {
		this.airportsDataTable = Table.create("Airports Data",
				StringColumn.create("icao"),
				DoubleColumn.create("longitude"),
				DoubleColumn.create("latitude"),
				DoubleColumn.create("elevation"));

	}

	public void appendRowToAirportsDataTable( AirportDataRecord record ) {

		Row row = this.airportsDataTable.appendRow();

		row.setString("icao", record.icao());
		row.setDouble("longitude", record.longitude());
		row.setDouble("latitude", record.latitude());
		row.setDouble("elevation", record.elevation());

	}
	
	public double getAirportDoubleValues( final String airport_icao_code , final String airportFieldName ) {
		
		Selection selection = this.airportsDataTable.stringColumn("icao").containsString(airport_icao_code);
		Table filtered = this.airportsDataTable.where(selection);
		
		DoubleColumn first = filtered.doubleColumn(airportFieldName ).first(1);
		
		double airportFloatValue = first.get(0);
		//System.out.println("airport = " + airport_icao_code + " --- longitude = " + Float.toString(airportLatitude) );
		return airportFloatValue;
	}

}
