package flightListData;

import flightData.FlightDataSchema.FlightDataRecord;
import flightListData.FlightListDataSchema.FlightListDataRecord;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

public class FlightListDataTable extends Table {

	public Table flightListDataTable = null;

	public Table getFlightListDataTable() {
		return flightListDataTable;
	}

	FlightListDataTable() {
		super("FlightListData");
	}

	public void createEmptyFlightListDataTable( ) {
		this.flightListDataTable = Table.create("Flight List Data",

				StringColumn.create("flight_id"),
				DateColumn.create("flight_date"),

				InstantColumn.create("takeoff"),

				StringColumn.create("origin_icao"),
				StringColumn.create("origin_name"),

				InstantColumn.create("landed"),

				StringColumn.create("destination_icao"),
				StringColumn.create("destination_name"),

				StringColumn.create("aircraft_type"));

	}

	public void appendRowToFlightListDataTable(  FlightListDataRecord r ) {

		Row row = this.flightListDataTable.appendRow();
		
		
		row.setString("flight_id", r.flight_id());
		row.setDate("flight_date", r.flight_date());
		
		row.setInstant("takeoff", r.takeoff());
		
		row.setString("origin_icao", r.origin_icao());
		row.setString("origin_name", r.origin_name());
		
		row.setInstant("landed", r.landed());

		row.setString("destination_icao", r.destination_icao());
		row.setString("destination_name", r.destination_name());

		row.setString("aircraft_type", r.aircraft_type());
		
	}
}
