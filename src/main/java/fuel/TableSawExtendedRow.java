package fuel;

import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

public class TableSawExtendedRow extends Row {
	
	public TableSawExtendedRow(final Table table) {
		super(table);
	}
	
	/**
	 * these modifications have been patched in the tablesaw Row implementation
	 */

	/**
	 * managing setDouble with null values
	 * @param columnName
	 * @param doubleValue
	 */
	public void setDouble( final String columnName , final double doubleValuePotentialNull ) {
		
		//Double DoubleValue = (Double)doubleValuePotentialNull;
		if ( (Double)doubleValuePotentialNull == null) {
			// do nothing -> creates a missing value in the table for this row and this column
			//this.setDouble( columnName, Double(null));
			
		} else {
			this.setDouble( columnName, doubleValuePotentialNull);
		}
	}
}
