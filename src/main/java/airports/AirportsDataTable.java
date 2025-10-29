package airports;

import airports.AirportsDataSchema.AirportDataRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;
import utils.Constants;

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
				FloatColumn.create("elevation"));

	}

	public void appendRowToAirportsDataTable( AirportDataRecord record ) {

		Row row = this.airportsDataTable.appendRow();

		assert record.icao()!= null;
		assert record.icao().length() > 0;
		row.setString("icao", record.icao());
		
		assert ( record.longitude() >= -180.0 );
		assert ( record.longitude() <= 180.0 );
		row.setDouble("longitude", record.longitude());
		
		assert ( record.latitude() >= -90.0 );
		assert ( record.latitude() <= 90.0 );
		row.setDouble("latitude", record.latitude());
		
		if ( record.elevation() < -1300.0 ) {
			System.out.println("airport ICAO = " + record.icao() + " --> elevation (feet) = " + record.elevation() + 
					" --> elevation meters = " + record.elevation() * Constants.feet_to_meters);
			row.setFloat("elevation", record.elevation());
		} else {
			// airport elevation above 4000.0 meters
			if ( record.elevation() >= (Constants.meter_to_feets * 5000.0 ) ) {
				System.out.println("airport ICAO = " + record.icao() + " --> elevation (feet) = " + record.elevation() + 
						" --> elevation meters = " + record.elevation() * Constants.feet_to_meters);
				row.setFloat("elevation", record.elevation());
			} else {
				assert ( record.elevation() <= (Constants.meter_to_feets * 5000.0 ));
				// expressed in feet
				row.setFloat("elevation", record.elevation());
			}
		}
	}
	
	public double getAirportDoubleValues( final String airport_icao_code , final String airportFieldName ) {
		
		Selection selection = this.airportsDataTable.stringColumn("icao").containsString(airport_icao_code);
		Table filtered = this.airportsDataTable.where(selection);
		
		DoubleColumn first = filtered.doubleColumn(airportFieldName ).first(1);
		
		double airportFloatValue = first.get(0);
		//System.out.println("airport = " + airport_icao_code + " --- longitude = " + Float.toString(airportLatitude) );
		return airportFloatValue;
	}
	
	public float getAirportFloatValues( final String airport_icao_code , final String airportFieldName ) {
		
		Selection selection = this.airportsDataTable.stringColumn("icao").containsString(airport_icao_code);
		Table filtered = this.airportsDataTable.where(selection);
		
		FloatColumn first = filtered.floatColumn(airportFieldName ).first(1);
		
		float airportFloatValue = first.get(0);
		return airportFloatValue;
	}

}
