package flights;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.NumberColumn;
import tech.tablesaw.api.Table;
import utils.CustomException;

public class Test_InterpolateOneFile_Test {

	/**
     * Performs simple linear interpolation on a NumberColumn.
     * NaN values are replaced by interpolated values between the nearest non-NaN neighbors.
     */
    public static void interpolateLinear(NumberColumn<DoubleColumn,Double> column) {
        int n = column.size();
        int i = 0;

        while (i < n) {
            // Skip non-missing values
            if (!Double.isNaN(column.getDouble(i))) {
                i++;
                continue;
            }

            // Start of missing sequence
            int start = i - 1; // index before missing
            int end = i;

            // Find end of missing sequence
            while (end < n && Double.isNaN(column.getDouble(end))) {
                end++;
            }

            // Interpolate only if we have valid values on both sides
            if (start >= 0 && end < n) {
                double startVal = column.getDouble(start);
                double endVal = column.getDouble(end);
                int gap = end - start;

                for (int j = 1; j < gap; j++) {
                    double interpolated = startVal + (endVal - startVal) * j / gap;
                    column.set(start + j, interpolated);
                }
            }
            // Move to next segment
            i = end;
        }
    }
	
	@Test
    public void Test_InterpolateOneFile_Test_one () throws IOException, CustomException {
				
		train_rank_final train_rank_final_value = train_rank_final.rank;
		
		System.out.println("================ test two searching for missing lat lon altitudes ==========================");
		
		FlightListData flightListData = new FlightListData(train_rank_final_value );
		flightListData.readParquet();
		
		System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );
		
		String rank_flight_id ="prc806642601";
		
		FlightData flightData = new FlightData(train_rank_final_value , rank_flight_id);
		flightData.readParquetWithStream();	
				
		Table flightDatatable = flightData.getFlightDataTable();
		//System.out.println(flightDatatable.print());
		System.out.println(flightDatatable.structure().print());
		
		// sort table using timestamp
		Table sortedFlightDatatable = flightDatatable.sortOn("timestamp");
		
		// all columns support some aggregate functions: min() and max(), for example, plus count(), countUnique(), and countMissing()
		for (String columnName : flightDatatable.columnNames()) {
			//System.out.println(columnName);
			
			Table localTableCopy = sortedFlightDatatable.copy();
			
			int countMissing = localTableCopy.column(columnName).countMissing();
			System.out.printf("Column %s - row count = %d -> number of missing %d \n" , columnName , localTableCopy.rowCount() , countMissing);
			
			for ( String tobeDroppedColumnName : flightDatatable.columnNames()) {
				if (  ! tobeDroppedColumnName.equalsIgnoreCase("flight_id") && 
						! tobeDroppedColumnName.equalsIgnoreCase("timestamp") && 
						! tobeDroppedColumnName.equalsIgnoreCase("source") && 
						 !tobeDroppedColumnName.equalsIgnoreCase(columnName)  ) {
					localTableCopy.removeColumns(tobeDroppedColumnName);

				}
			}
			System.out.println(localTableCopy.structure().print());

		}
		
	}
}
