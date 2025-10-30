package flights;

import java.io.IOException;
import java.time.Instant;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.junit.jupiter.api.Test;

import tech.tablesaw.aggregate.AggregateFunctions;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.joining.DataFrameJoiner;

public class Test_SummarizeSuppressDuplicatedInstants_Test {

	@Test
	public void testTableSummarize () throws IOException {		

		System.out.println("--------- start test summarize -----");

		double[] numbers = {8, 7, 10, 11};

		DoubleColumn nc = DoubleColumn.create("nc", numbers);
		System.out.println(nc.print());

		Instant instant1 = Instant.parse("2020-04-09T10:15:30.00Z");
		System.out.println("Instant1: " + instant1);

		Instant instant2 = Instant.parse("2025-04-09T10:15:32.00Z");
		System.out.println("Instant2: " + instant2);

		Instant instant3 = Instant.parse("2025-09-10T10:15:32.00Z");
		System.out.println("Instant3: " + instant3);

		// second instant with identical instant
		Instant instant4 = Instant.parse("2025-09-10T10:15:32.00Z");
		System.out.println("Instant4: " + instant4);

		Instant[] xInstants = new Instant[] { instant1 , instant2 , instant3 , instant4 };

		InstantColumn xt = InstantColumn.create("timestamp" , xInstants);
		System.out.println(xt.print());

		Table table = Table.create("test");
		table.addColumns(xt);
		table.addColumns(nc);

		System.out.println(table.print());
		assert ( table.columnNames().contains("timestamp") );
		assert ( table.columnNames().contains("nc") );
		
		int sizeOfArray = numbers.length;
		
		double[] xSeconds = new double[sizeOfArray];
		// convert Instant to seconds (long)
		for ( int i = 0 ; i < sizeOfArray ; i++) {
			xSeconds[i] = xInstants[i].getEpochSecond();
		}
		
		LinearInterpolator interpolator = new LinearInterpolator();
		try {
			PolynomialSplineFunction function = interpolator.interpolate(xSeconds, numbers);
			Instant testInstant = Instant.parse("2025-06-10T10:15:32.00Z");
			double testDouble = testInstant.getEpochSecond();
			function.isValidPoint(testDouble);
		} catch ( Exception e) {
			System.out.println("==== should have an exception here because 2 X points are identical ====");
			System.out.println("Exception during interpolation -> " + e.getLocalizedMessage());
		}

		// take only double feature column value for the first time-stamp (in case of duplicates)
		Table filteredTable = table.summarize("nc", AggregateFunctions.first).by("timestamp");
		System.out.println("after summarize = " +  filteredTable.print());
		
		// sort table based upon time-stamp
		filteredTable = filteredTable.sortAscendingOn("timestamp");
		System.out.println("after sort ascending = " +  filteredTable.print());
		
		filteredTable.column(1).setName( "nc_for_interpolation" );
		System.out.println( filteredTable.structure().print());
		System.out.println( filteredTable.print());
		

		

	}
}
