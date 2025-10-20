package flightData;

import tech.tablesaw.api.*; 
import flightData.FlightDataSchema.FlightDataRecord;

public class FlightDataTable extends Table {
	
	public Table flightDataTable = null;

	public Table getFlightDataTable() {
		return flightDataTable;
	}
	
	FlightDataTable() {
		super("FlightData");
	}

	public void createEmptyFlightDataTable( ) {
		this.flightDataTable = Table.create("Flight Data",
                StringColumn.create("flight_id"),
                InstantColumn.create("timestamp"),
                FloatColumn.create("longitude"),
                FloatColumn.create("latitude"),
                FloatColumn.create("altitude"),
                FloatColumn.create("groundspeed"),
                FloatColumn.create("track"),
                FloatColumn.create("vertical_rate"),
                FloatColumn.create("mach"),
                StringColumn.create("typecode"),
                FloatColumn.create("TAS"),
                FloatColumn.create("CAS"),
				StringColumn.create("source"));
		
	}
	
	public void appendRowToFlightDataTable(  FlightDataRecord r ) {
		
		Row row = this.flightDataTable.appendRow();
        row.setString("flight_id", r.flight_id());
        row.setInstant("timestamp", r.timestamp());
        row.setFloat("longitude", r.longitude());
        row.setFloat("latitude", r.latitude());
        row.setFloat("altitude", r.altitude());
        row.setFloat("groundspeed", r.groundspeed());
        row.setFloat("vertical_rate", r.vertical_rate());
        row.setFloat("mach", r.mach());
        row.setString("typecode", r.typecode());
        row.setFloat("TAS", r.TAS());
        row.setFloat("CAS", r.CAS());
        row.setString("source", r.source());
	}
	
}
