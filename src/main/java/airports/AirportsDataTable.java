package airports;

import airports.AirportsDataSchema.AirportDataRecord;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

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

}
