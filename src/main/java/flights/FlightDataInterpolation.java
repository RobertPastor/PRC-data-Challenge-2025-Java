package flights;

import java.time.Instant;
import java.util.Arrays;
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

	public FlightDataInterpolation(final List<String> columnsAsInterpolationSource) {
		logger.info("---- constructor ----");
		this.setListOfFlightColumnsToInterpolate(columnsAsInterpolationSource);
	}
/**
 * buil
 * @param flightDataTable
 */
	public void buildInterpolationFunctions( final Table flightDataTable) {

		logger.info("---- build interpolation functions -----");
		// create the interpolation functions
		
		for ( String columnName : this.listOfFlightDataColumnsToInterpolate) {
			logger.info("build interpolation function for column name = " + columnName);
			System.out.println( flightDataTable.shape());
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
		
		System.out.println( columnName );
		System.out.println( flightDataTable.shape());

		// filter the table with a selection
		Selection selectionNotMissing = flightDataTable.doubleColumn(columnName).isNotMissing();
		Table filteredTable = flightDataTable.where(selectionNotMissing);
		
		// sort table based upon timestamp
		filteredTable = filteredTable.sortAscendingOn("timestamp");

		// create a list of instants as X values in the interpolation
		Instant[] xInstants =  (Instant[]) filteredTable.column("timestamp").unique().asObjectArray();

		int sizeOfArray = xInstants.length;
		double[] yValues = new double[sizeOfArray];

		// take only first timestamp
		filteredTable = filteredTable.summarize(columnName, AggregateFunctions.first).by("timestamp");
		
		System.out.println( filteredTable.print(10));
		
		// rename second column
		String columnNameToInterpolate = columnName + "_interpolated";
		filteredTable.column(1).setName( columnNameToInterpolate );
		System.out.println( filteredTable.structure().print());

		DoubleColumn c = (DoubleColumn) filteredTable.column(columnNameToInterpolate);
		yValues = c.asDoubleArray();

		double[] xMilliSeconds = new double[sizeOfArray];
		// convert Instant to seconds (long)
		for ( int i = 0 ; i < xInstants.length ; i++) {
			xMilliSeconds[i] = xInstants[i].toEpochMilli();
		}
		// Perform interpolation

		LinearInterpolator interpolator = new LinearInterpolator();
		PolynomialSplineFunction function = interpolator.interpolate(xMilliSeconds, yValues);

		// put the function in the map
		interpolationFunctionMap.put ( columnName , function);
	}

	/**
	 * get double value from an start end input given by the fuel data frame
	 * @param columnName
	 * @param start_end
	 * @return
	 */
	public double getDoubleFlightDataAtInterpolatedStartEndFuelInstant(final String columnName , final Instant start_end) {
		
		// use interpolation function when start end instant inside the range of the function
		// 	private Map<String, PolynomialSplineFunction> interpolationFunctionMap = null;
		
		if ( interpolationFunctionMap.containsKey(columnName)) {
			logger.info("Interpolation map contains an interpolation function for the column = " + columnName);
		} else {
			assert (false);
		}

		PolynomialSplineFunction interpolateFunction = (PolynomialSplineFunction)interpolationFunctionMap.get(columnName);
		double queryMilliSeconds = start_end.toEpochMilli();

		if (  interpolateFunction.isValidPoint(queryMilliSeconds)) {
			return interpolateFunction.value(queryMilliSeconds);
		}
        return 0.0;
		
	}
	
	/**
	 * warning use milliseconds to avoid monotonic exception from the math3 interpolation function
	 * @param columnName
	 * @param start_end
	 * @return
	 */
	public float getFloatFlightDataAtInterpolatedStartEndFuelInstant(final String columnName , final Instant start_end) {
		
		if ( interpolationFunctionMap.containsKey(columnName)) {
			logger.info("Interpolation map contains an interpolation function for the column = " + columnName);
		} else {
			assert (false);
		}
		
		PolynomialSplineFunction function = (PolynomialSplineFunction)interpolationFunctionMap.get(columnName);
		double queryMilliSeconds = start_end.toEpochMilli();

		if ( function.isValidPoint( queryMilliSeconds )) {
			return (float)function.value(queryMilliSeconds);
		} else {
			return (float) 0.0;
		}
       
	}
	
}
