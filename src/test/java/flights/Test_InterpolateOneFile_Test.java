package flights;

import java.io.IOException;
import java.time.Instant;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import utils.CustomException;

public class Test_InterpolateOneFile_Test {

	private void interpolate(Table tableToInterpolate , final String columnNameToInterpolate) {
		
		// sort again
		tableToInterpolate = tableToInterpolate.sortOn("timestamp");
		System.out.println(tableToInterpolate.structure().print());
		System.out.println(tableToInterpolate.shape());

		// take only double feature column value for the first time-stamp (in case of duplicates)
		// suppress duplicates in timestap row 
		//Summarizer summarizer =  tableToInterpolate.summarize(columnNameToInterpolate, AggregateFunctions.min);
				
		tableToInterpolate = tableToInterpolate.summarize(columnNameToInterpolate, AggregateFunctions.first).by("timestamp");
		tableToInterpolate.column(1).setName(columnNameToInterpolate);

		System.out.println(tableToInterpolate.structure().print());
		// warning InstantColumn in tablesaw are down to milliseconds not nanoseconds as the java.time.Instant
		tableToInterpolate = tableToInterpolate.sortOn("timestamp");
		System.out.println(tableToInterpolate.print());
		
		// rename column with index = 1
		int rowCount = tableToInterpolate.rowCount();
		
		System.out.println(tableToInterpolate.structure().print());
		System.out.println(tableToInterpolate.shape());

		InstantColumn columnInstant = (InstantColumn) tableToInterpolate.column("timestamp");
		double[] xMilliseconds = new double[rowCount];
		
		// convert Instant to seconds (long)
		for ( int i = 0 ; i < rowCount ; i++) {
			// to avoid duplicates and error in math3 interpolate need to convert Instant to nano seconds
			// converting to seconds will produce identifical consecutives seconds
			// instant column with milliseconds precision unlike java.time down to nanoseconds			
			Instant instant =  columnInstant.get(i);

			long timeStampAsLong = instant.toEpochMilli();
			xMilliseconds[i] = (double)timeStampAsLong;
			//System.out.println(String.valueOf(timeStampAsLong));
		}

		// build a double array as expected by the Apache.math3 interpolation function
		// transform column to interpolate into an array of double
		DoubleColumn columnDouble = (DoubleColumn) tableToInterpolate.column(columnNameToInterpolate);
		double[] yValues = new double[rowCount];
		yValues = columnDouble.asDoubleArray();
		
		LinearInterpolator interpolator = new LinearInterpolator();
		PolynomialSplineFunction interpolateFunction = interpolator.interpolate(xMilliseconds, yValues);

		//  interpolation of holes / empty / Not a numbe
		for (int i = 0; i < rowCount; i++) {
			if (!Double.isNaN(yValues[i])) {

				double instantMilliSecondsTToInterpolate = xMilliseconds[i];
				if (  interpolateFunction.isValidPoint(instantMilliSecondsTToInterpolate)) {
					yValues[i] = interpolateFunction.value(instantMilliSecondsTToInterpolate);
				} 
			}
		}
		Table interpolatedTable = Table.create( )
		return interpolatedTable;
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
		Table localTableCopy = sortedFlightDatatable.copy().sortOn("timestamp");

		// all columns support some aggregate functions: min() and max(), for example, plus count(), countUnique(), and countMissing()
		for (String columnNameToInterpolate : flightDatatable.columnNames()) {
			if ( ! columnNameToInterpolate.equalsIgnoreCase("source") && ! columnNameToInterpolate.equalsIgnoreCase("typecode") 
					&& ! columnNameToInterpolate.equalsIgnoreCase("flight_id") && ! columnNameToInterpolate.equalsIgnoreCase("timestamp") ) {
				// only number columns to interpolate
				//System.out.println(columnName);

				int countMissing = localTableCopy.column(columnNameToInterpolate).countMissing();
				boolean columnToInterpolateIsEmpty = localTableCopy.column(columnNameToInterpolate).isEmpty();
				if ( ( countMissing == localTableCopy.rowCount() ) || columnToInterpolateIsEmpty  ) {
					// the whole column is empty -> impossible to interpolate

					System.out.printf("Column %s - row count = %d -> number of missing %d \n" , columnNameToInterpolate , localTableCopy.rowCount() , countMissing);
					System.out.println("--- cannot interpolate a column that is empty ---");
					return;
				} else {
					System.out.println("--- <<" +  columnNameToInterpolate + ">> to interpolate ---");
					// drop all other columns - should keep timestamp and the column to interpolate
					
					Table tableToInterpolate = localTableCopy.selectColumns("timestamp",columnNameToInterpolate);
					//System.out.println(tableToInterpolate.structure().print());

					Table interpolatedTable = this.interpolate(tableToInterpolate , columnNameToInterpolate);
				}
			}
		}
	}
}