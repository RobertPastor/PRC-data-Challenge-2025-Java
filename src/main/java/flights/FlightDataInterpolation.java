package flights;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;

public class FlightDataInterpolation {

	public class InterpolationFunctionNotFoundException extends RuntimeException {
		/**
		 * exception raised when a an interpolation function not found for a column name
		 * of the Flight data such altitude, latitude, longitude, etc.
		 */
		private static final long serialVersionUID = 1L;
		public InterpolationFunctionNotFoundException(String message) {
			super(message);
		}
	}

	private static final Logger logger = Logger.getLogger(FlightDataInterpolation.class.getName());

	List<String> listOfFlightDataColumnsToInterpolate ;

	public List<String> getListOfFlightColumnsToInterpolate() {
		return listOfFlightDataColumnsToInterpolate;
	}

	public void setListOfFlightColumnsToInterpolate(List<String> listOfFlightColumnsToInterpolate) {
		this.listOfFlightDataColumnsToInterpolate = listOfFlightColumnsToInterpolate;
	}

	// map of interpolation functions for each interpolated column
	protected Map<String, PolynomialSplineFunction> interpolationFunctionMap = null;

	public Map<String, PolynomialSplineFunction> getInterpolationFunctionMap() {
		return interpolationFunctionMap;
	}

	/**
	 * constructor
	 * @param columnsAsInterpolationSource
	 */
	public FlightDataInterpolation(final List<String> columnsAsInterpolationSource) {
		//logger.info("---- constructor ----");
		this.setListOfFlightColumnsToInterpolate(columnsAsInterpolationSource);
		// initialise an empty map of interpolation functions
		interpolationFunctionMap = new HashMap<String, PolynomialSplineFunction>();
	}
	/**
	 * build the interpolation function for each double/float column of the Flight Data frame
	 * @param flightDataTable
	 */
	public void buildInterpolationFunctions( final Table flightDataTable) {

		//logger.info("---- build interpolation functions -----");
		// create the interpolation functions

		for ( String columnName : this.listOfFlightDataColumnsToInterpolate) {
			//logger.info("build interpolation function for column name = " + columnName);
			//System.out.println( flightDataTable.shape());
			this.generateInterpolationFunction( columnName , flightDataTable);
		}
	}

	/**
	 * warning use milliseconds for the x axis of the polynomial function
	 * @param columnName
	 * @param flightDataTable
	 * @return
	 */
	public void  generateInterpolationFunction ( final String columnName , 
			final Table flightDataTable) {

		//System.out.println( columnName );
		//System.out.println( flightDataTable.shape());

		// filter the table with a selection
		// filter missing values in the selected column
		Selection selectionNotMissing = flightDataTable.doubleColumn(columnName).isNotMissing();
		Table filteredTable = flightDataTable.where(selectionNotMissing);

		// take only double feature column value for the first time-stamp (in case of duplicates)
		filteredTable = filteredTable.summarize(columnName, AggregateFunctions.first).by("timestamp");

		// case where the filtered table is empty - no use
		if ( filteredTable.rowCount() > 0) {
			// sort table based upon time-stamp
			filteredTable = filteredTable.sortAscendingOn("timestamp");
			
			//System.out.println("after sorting = " + filteredTable.print());

			// create a list of instants as X values in the interpolation
			// instant after the filtering done using Summarize
			Instant[] xInstants =  (Instant[]) filteredTable.column("timestamp").asObjectArray();

			int sizeOfArray = xInstants.length;
			// build a double array as expected by the Apache.math3 interpolation function
			double[] yValues = new double[sizeOfArray];

			//System.out.println( filteredTable.print(10));

			// second column as id = 1
			int secondColumnId = 1;
			filteredTable.column(secondColumnId).setName( columnName );
			//System.out.println( filteredTable.structure().print());

			DoubleColumn c = (DoubleColumn) filteredTable.column(columnName);
			yValues = c.asDoubleArray();

			double[] xMilliSeconds = new double[sizeOfArray];
			// convert Instant to seconds (long)
			for ( int i = 0 ; i < xInstants.length ; i++) {
				xMilliSeconds[i] = xInstants[i].toEpochMilli();
				//if ( i < 3 ) {
					//System.out.println(xMilliSeconds[i]);
				//}
			}
			// Perform interpolation
			try {
				LinearInterpolator interpolator = new LinearInterpolator();
				PolynomialSplineFunction function = interpolator.interpolate(xMilliSeconds, yValues);
				// put the function in the map
				interpolationFunctionMap.put ( columnName , function);
			} catch ( Exception e) {
				System.out.println( "Exception during interpolation -> " + e.getLocalizedMessage());
			}
		}
	}
	
	/**
	 * get double value from an start end input given by the fuel data frame
	 * @param columnName
	 * @param start_end
	 * @return
	 */
	public Double getDoubleFlightDataAtInterpolatedStartEndFuelInstant(final String columnName , final Instant start_end) {

		// use interpolation function when start end instant inside the range of the function
		// 	private Map<String, PolynomialSplineFunction> interpolationFunctionMap = null;

		if ( interpolationFunctionMap.containsKey(columnName)) {
			//logger.info("Interpolation map contains an interpolation function for the column = " + columnName);
		} else {
			return (Double)null;
			//throw new InterpolationFunctionNotFoundException("Interpolation DOUBLE function for column = "+ columnName + " ---> not found in them= map");
		}
		try {
			PolynomialSplineFunction interpolateFunction = (PolynomialSplineFunction)interpolationFunctionMap.get(columnName);
			double queryMilliSeconds = start_end.toEpochMilli();

			if (  interpolateFunction.isValidPoint(queryMilliSeconds)) {
				return interpolateFunction.value(queryMilliSeconds);
			} else {
				return (Double)null;
			}
		} catch (Exception e) {
			System.out.println("Exception while interpolating -> " + e.getLocalizedMessage());
		}
		return (Double)null;
	}

	/**
	 * warning use milliseconds to avoid monotonic exception from the math3 interpolation function
	 * @param columnName
	 * @param start_end
	 * @return
	 */
	public float getFloatFlightDataAtInterpolatedStartEndFuelInstant(final String columnName , final Instant start_end) {

		if ( interpolationFunctionMap.containsKey(columnName)) {
			// logger.info("Interpolation map contains an interpolation function for the column = " + columnName);
		} else {
			return (float)0.0;
			//throw new InterpolationFunctionNotFoundException("Interpolation FLOAT function for column = "+ columnName + " ---> not found in them= map");
		}

		try {
			PolynomialSplineFunction function = (PolynomialSplineFunction)interpolationFunctionMap.get(columnName);
			double queryMilliSeconds = start_end.toEpochMilli();

			if ( function.isValidPoint( queryMilliSeconds )) {
				return (float)function.value(queryMilliSeconds);
			} else {
				return (float) 0.0;
			}
		}	catch (Exception e) {
			System.out.println("Exception while interpolating -> " + e.getLocalizedMessage());
		}
		return (float) 0.0;
	}
}
