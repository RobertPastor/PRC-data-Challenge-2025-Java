package airports;

import airports.AirportsDataSchema.AirportDataRecord;
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
				FloatColumn.create("longitude"),
				FloatColumn.create("latitude"),
				FloatColumn.create("elevation"));

	}

	public void appendRowToAirportsDataTable(  AirportDataRecord r ) {

		Row row = this.airportsDataTable.appendRow();

		row.setString("icao", r.icao());
		row.setFloat("longitude", r.longitude());
		row.setFloat("latitude", r.latitude());
		row.setFloat("elevation", r.elevation());

	}
	
	public float getAirportFloatValues( final String airport_icao_code , final String airportFieldName ) {
		
		Selection selection = this.airportsDataTable.stringColumn("icao").containsString(airport_icao_code);
		Table filtered = this.airportsDataTable.where(selection);
		
		FloatColumn first = filtered.floatColumn(airportFieldName ).first(1);
		
		float airportFloatValue = first.get(0);
		//System.out.println("airport = " + airport_icao_code + " --- longitude = " + Float.toString(airportLatitude) );
		return airportFloatValue;
	}

}
