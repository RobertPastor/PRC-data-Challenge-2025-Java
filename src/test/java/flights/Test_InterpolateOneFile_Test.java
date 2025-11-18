package flights;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import utils.CustomException;

public class Test_InterpolateOneFile_Test {
	
	private String getInterpolatedColumnTableName (final train_rank_final train_rank_final_value ,  
			final String flight_id ,  final String columnNameToInterpolate) {
		return train_rank_final_value + "_" + flight_id + "_" + columnNameToInterpolate;
	}
	
	private Table suppressDuplicatedTimeStampsinStringColumn( final train_rank_final train_rank_final_value, 
			final String flight_id , Table tableToInterpolate , final String columnNameToInterpolate) {
		
		tableToInterpolate = tableToInterpolate.sortOn("timestamp");
		
		Table interpolatedTable = Table.create(this.getInterpolatedColumnTableName(train_rank_final_value, flight_id, columnNameToInterpolate) );

		InstantColumn instantColumn = (InstantColumn) tableToInterpolate.column("timestamp");
		interpolatedTable = interpolatedTable.addColumns(instantColumn);
		StringColumn stringColumn = (StringColumn) tableToInterpolate.column(columnNameToInterpolate);
		interpolatedTable = interpolatedTable.addColumns(stringColumn);

		tableToInterpolate = tableToInterpolate.dropDuplicateRows();		
		
		System.out.println("Interpolated resulting table shape " + interpolatedTable.shape());
		System.out.println("Interpolated resulting table structure " + interpolatedTable.structure().print());
		return tableToInterpolate;
	}
	
	private Table suppressDuplicatedTimeStamps( final train_rank_final train_rank_final_value, 
			final String flight_id , Table tableToInterpolate , final String columnNameToInterpolate) {
		
		tableToInterpolate = tableToInterpolate.sortOn("timestamp");

		tableToInterpolate = tableToInterpolate.summarize(columnNameToInterpolate, AggregateFunctions.first).by("timestamp");
		// rename column with index = 1	-> it is the summarized column		// rename column with index = 1	-> it is the summarized column
		tableToInterpolate.column(1).setName(columnNameToInterpolate);
		tableToInterpolate = tableToInterpolate.sortOn("timestamp");
		
		Table interpolatedTable = Table.create(this.getInterpolatedColumnTableName(train_rank_final_value, flight_id, columnNameToInterpolate) );
		
		InstantColumn instantColumn = (InstantColumn) tableToInterpolate.column("timestamp");
		interpolatedTable = interpolatedTable.addColumns(instantColumn);
		
		DoubleColumn doubleColumn = (DoubleColumn) tableToInterpolate.column(columnNameToInterpolate);
		interpolatedTable = interpolatedTable.addColumns(doubleColumn);

		System.out.println("Interpolated resulting table shape " + interpolatedTable.shape());
		System.out.println("Interpolated resulting table structure " + interpolatedTable.structure().print());
		return interpolatedTable;

	}

	private Table interpolate(final train_rank_final train_rank_final_value, final String flight_id ,
			Table tableToInterpolate , final String columnNameToInterpolate) {
		
		// sort again
		tableToInterpolate = tableToInterpolate.sortOn("timestamp");
		//System.out.println(tableToInterpolate.structure().print());
		//System.out.println(tableToInterpolate.shape());

		// take only double feature column value for the first time-stamp (in case of duplicates)
		// suppress duplicates in timestap row 
		//Summarizer summarizer =  tableToInterpolate.summarize(columnNameToInterpolate, AggregateFunctions.min);
				
		tableToInterpolate = tableToInterpolate.summarize(columnNameToInterpolate, AggregateFunctions.first).by("timestamp");
		// rename column with index = 1	-> it is the summarized column
		tableToInterpolate.column(1).setName(columnNameToInterpolate);

		// warning InstantColumn in tablesaw are down to milliseconds not nanoseconds as the java.time.Instant
		tableToInterpolate = tableToInterpolate.sortOn("timestamp");
		
		int rowCount = tableToInterpolate.rowCount();
		
		//System.out.println(tableToInterpolate.structure().print());
		//System.out.println(tableToInterpolate.shape());

		InstantColumn instantColumn = (InstantColumn) tableToInterpolate.column("timestamp");
		double[] xMilliseconds = new double[rowCount];
		
		// convert Instant to seconds (long)
		for ( int i = 0 ; i < rowCount ; i++) {
			// to avoid duplicates and error in math3 interpolate need to convert Instant to nano seconds
			// converting to seconds will produce identifical consecutives seconds
			// instant column with milliseconds precision unlike java.time down to nanoseconds			
			Instant instant =  instantColumn.get(i);

			long timeStampAsLong = instant.toEpochMilli();
			xMilliseconds[i] = (double)timeStampAsLong;
			//System.out.println(String.valueOf(timeStampAsLong));
		}

		// build a double array as expected by the Apache.math3 interpolation function
		// transform column to interpolate into an array of double
		DoubleColumn doubleColumn = (DoubleColumn) tableToInterpolate.column(columnNameToInterpolate);
		double[] yValues = new double[rowCount];
		yValues = doubleColumn.asDoubleArray();
		
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
		Table interpolatedTable = Table.create(this.getInterpolatedColumnTableName(train_rank_final_value, flight_id, columnNameToInterpolate) );
		interpolatedTable = interpolatedTable.addColumns(instantColumn);
		interpolatedTable = interpolatedTable.addColumns(doubleColumn);
		
		System.out.println("Interpolated resulting table shape " + interpolatedTable.shape());
		System.out.println("Interpolated resulting table structure " + interpolatedTable.structure().print());

		//System.out.println(interpolatedTable.print());
		return interpolatedTable;
	}

	@SuppressWarnings("deprecation")
	@Test
	public void Test_InterpolateOneFile_Test_one () throws IOException, CustomException {

		train_rank_final train_rank_final_value = train_rank_final.rank;

		System.out.println("================ interpolate column that are not empty ==========================");
		System.out.println("================ keep other columns as they are ==========================");
		System.out.println("================ filter duplicated TimeStamps based upon their millisecond values ==========================");

		FlightListData flightListData = new FlightListData(train_rank_final_value );
		flightListData.readParquet();

		//System.out.println("shape = " + flightListData.getFlightListDataTable().shape() );

		String flight_id = "prc806642601";

		FlightData flightData = new FlightData(train_rank_final_value , flight_id);
		flightData.readParquetWithStream();	

		Table flightDatatable = flightData.getFlightDataTable();
		//System.out.println(flightDatatable.print());
		//System.out.println(flightDatatable.structure().print());

		// sort table using timestamp
		Table sortedFlightDatatable = flightDatatable.sortOn("timestamp");
		Table localTableCopy = sortedFlightDatatable.copy().sortOn("timestamp");
		
		// the table that will be written in the Interpolated folder in relation to the train, rank or final enum value
		Table finalInterpolatedTable = Table.create(train_rank_final_value + "_" + flight_id + "_" + "interpolated");
		
		boolean first = true;
		// create a table with the string column as suppress duplicated timestamps to reach the same row count as for 
		// summarized double columns
		for ( String stringColumn : List.of("flight_id","source","typecode")) {
			
			Table tableToInterpolate = localTableCopy.selectColumns("timestamp",stringColumn);
			Table interpolatedTable = this.suppressDuplicatedTimeStampsinStringColumn(train_rank_final_value, flight_id, 
					tableToInterpolate, stringColumn);
			if ( first ) {
				finalInterpolatedTable = interpolatedTable;
				first = false;

			} else {
				finalInterpolatedTable = finalInterpolatedTable.joinOn("timestamp").leftOuter(interpolatedTable);
			}
			System.out.println("final interpolated table -> " + finalInterpolatedTable.shape());
			System.out.println("final interpolated table -> " + finalInterpolatedTable.structure().print());
		}
		

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

					System.out.printf("Column <<%s>> - row count = <<%d>> -> number of missing <<%d>> \n" , columnNameToInterpolate , localTableCopy.rowCount() , countMissing);
					System.out.println("--- cannot interpolate a column <<" + columnNameToInterpolate + ">> that is empty ---");
					// the filtered table may have less rows because of duplicated timestamps
					Table tableToInterpolate = localTableCopy.selectColumns("timestamp",columnNameToInterpolate);

					Table interpolatedTable = this.suppressDuplicatedTimeStamps(train_rank_final_value, flight_id, 
							tableToInterpolate, columnNameToInterpolate);
					
					finalInterpolatedTable = interpolatedTable.joinOn("timestamp").leftOuter(finalInterpolatedTable);
					System.out.println(finalInterpolatedTable.shape());
					
				} else {
					System.out.println("--- <<" +  columnNameToInterpolate + ">> ready to interpolate ---");
					// drop all other columns - should keep timestamp and the column to interpolate
					
					Table tableToInterpolate = localTableCopy.selectColumns("timestamp",columnNameToInterpolate);
					
					// the interpolated table may have less rows
					Table interpolatedTable = this.interpolate(train_rank_final_value , flight_id , tableToInterpolate , columnNameToInterpolate);
					
					finalInterpolatedTable = interpolatedTable.joinOn("timestamp").leftOuter(finalInterpolatedTable);
					System.out.println(finalInterpolatedTable.structure().print());
				}
			}
		}
		System.out.println("final interpolated table -> " + finalInterpolatedTable.shape());
		System.out.println("final interpolated table -> " + finalInterpolatedTable.structure().print());
		FlightDataTable flightDataTable = new FlightDataTable(finalInterpolatedTable);
		flightDataTable.generateParquetFileFor(train_rank_final_value , flight_id);
	}
}