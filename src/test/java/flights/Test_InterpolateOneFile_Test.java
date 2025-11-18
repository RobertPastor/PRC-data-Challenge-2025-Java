package flights;

import java.io.IOException;
import java.time.Instant;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.aggregate.Summarizer;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.NumberColumn;
import tech.tablesaw.api.Table;
import utils.CustomException;

public class Test_InterpolateOneFile_Test {

	private void interpolate(Table tableToInterpolate , final String columnNameToInterpolate) {
		
		// sort again
		tableToInterpolate = tableToInterpolate.sortOn("timestamp");
		System.out.println(tableToInterpolate.structure().print());

		// take only double feature column value for the first time-stamp (in case of duplicates)
		// suppress duplicates in timestap row 
		//Summarizer summarizer =  tableToInterpolate.summarize(columnNameToInterpolate, AggregateFunctions.min);
				
		tableToInterpolate = tableToInterpolate.summarize(columnNameToInterpolate, AggregateFunctions.first).by("timestamp");
		
		System.out.println(tableToInterpolate.structure().print());
		//localTableCopy = localTableCopy.selectColumns("timestamp",columnNameToInterpolate).dropDuplicateRows();
		
		// rename column with index = 1
		tableToInterpolate.column(1).setName(columnNameToInterpolate);
		System.out.println(tableToInterpolate.structure().print());

		Instant[] xInstants =  (Instant[]) tableToInterpolate.column("timestamp").asObjectArray();
		int sizeOfArray = xInstants.length;

		double[] xSeconds = new double[sizeOfArray];
		// convert Instant to seconds (long)
		for ( int i = 0 ; i < xInstants.length ; i++) {
			xSeconds[i] = xInstants[i].getEpochSecond();
		}

		// build a double array as expected by the Apache.math3 interpolation function
		// transform column to interpolate into an array of doubble
		DoubleColumn doubleCol = (DoubleColumn) tableToInterpolate.column(columnNameToInterpolate);
		double[] yValues = new double[sizeOfArray];
		yValues = doubleCol.asDoubleArray();

		LinearInterpolator interpolator = new LinearInterpolator();
		PolynomialSplineFunction interpolateFunction = interpolator.interpolate(xSeconds, yValues);

		// forward fill interpolation
		double[] yInterpolatedValues = new double[sizeOfArray];

		for (int i = 0; i < sizeOfArray; i++) {
			if (!Double.isNaN(yValues[i])) {

				double instantSecTToInterpolate = xSeconds[i];
				if (  interpolateFunction.isValidPoint(instantSecTToInterpolate)) {
					yInterpolatedValues[i] = interpolateFunction.value(instantSecTToInterpolate);
				} 
			} else {
				yInterpolatedValues[i] = yValues[i];
			}
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
		Table localTableCopy = sortedFlightDatatable.copy().sortOn("timestamp");

		// all columns support some aggregate functions: min() and max(), for example, plus count(), countUnique(), and countMissing()
		for (String columnNameToInterpolate : flightDatatable.columnNames()) {
			if ( ! columnNameToInterpolate.equalsIgnoreCase("source") && ! columnNameToInterpolate.equalsIgnoreCase("typecode") 
					&& ! columnNameToInterpolate.equalsIgnoreCase("flight_id") && ! columnNameToInterpolate.equalsIgnoreCase("timestamp") ) {
				// only number columns to interpolate
				//System.out.println(columnName);

				int countMissing = localTableCopy.column(columnNameToInterpolate).countMissing();

				if ( countMissing == localTableCopy.rowCount() ) {
					// the whole column is empty -> impossible to interpolate

					System.out.printf("Column %s - row count = %d -> number of missing %d \n" , columnNameToInterpolate , localTableCopy.rowCount() , countMissing);
					System.out.println("--- cannot interpolate a column that is empty ---");
					return;
				} else {
					System.out.println("--- <<" +  columnNameToInterpolate + ">> to interpolate ---");
					// drop all other columns - should keep timestamp and the column to interpolate
					
					Table tableToInterpolate = localTableCopy.selectColumns("timestamp",columnNameToInterpolate);
					System.out.println(tableToInterpolate.structure().print());

					this.interpolate(tableToInterpolate , columnNameToInterpolate);
				}
			}
		}
	}
}