package flights;

import java.io.IOException;
import java.time.Instant;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.junit.jupiter.api.Test;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.Table;

public class Test_TableSortingOnInstants_Test {

	@Test
	public void testTableFilterMissingValues () throws IOException {		

		System.out.println("--------- start test filter missing values -----");
	
		double[] numbers = {1, 2, 3, 4};
		int arraySize = numbers.length;
		
		DoubleColumn nc = DoubleColumn.create("nc", numbers);
		System.out.println(nc.print());

		Instant instant1 = Instant.parse("2025-04-09T10:15:30.00Z");
		System.out.println("Instant1: " + instant1);

		Instant instant2 = Instant.parse("2025-03-09T10:15:32.00Z");
		System.out.println("Instant2: " + instant2);

		Instant instant3 = Instant.parse("2025-05-10T10:15:32.00Z");
		System.out.println("Instant3: " + instant3);

		Instant instant4 = Instant.parse("2025-09-10T10:15:32.00Z");
		System.out.println("Instant4: " + instant4);
		
		Instant[] xInstants = new Instant[] { instant1 , instant2 , instant3 , instant4 };

		InstantColumn xt = InstantColumn.create("xt" , xInstants);
		System.out.println(xt.print());
		
		double[] xSeconds = new double[arraySize];
		
		for ( int i = 0 ; i < xt.size() ; i++ ) {
			xSeconds[i] = (double)xInstants[i].getEpochSecond();
		}

		Table table = Table.create("test");
		table.addColumns(xt);
		table.addColumns(nc);
		
		System.out.println( table.print() );
		
		LinearInterpolator interpolator = new LinearInterpolator();
		try {
			PolynomialSplineFunction function = interpolator.interpolate(xSeconds, numbers);
			Instant testInstant = Instant.parse("2025-06-10T10:15:32.00Z");
			double testDouble = testInstant.getEpochSecond();
			function.isValidPoint(testDouble);
		} catch ( Exception e) {
			System.out.println("Exception during interpolation -> " + e.getLocalizedMessage());
		}
		
		Table sortedTable = table.sortAscendingOn("xt");
		System.out.println( sortedTable.print() );
		
		InstantColumn xtSorted = sortedTable.instantColumn("xt");
		System.out.println( xtSorted.print() );
		
		for ( int i = 0 ; i < xt.size() ; i++ ) {
			xSeconds[i] = (double)xtSorted.get(i).getEpochSecond();
		}
		System.out.println( xSeconds );

		try {
			PolynomialSplineFunction function = interpolator.interpolate(xSeconds, numbers);
			Instant testInstant = Instant.parse("2025-06-10T10:15:32.00Z");
			double testDouble = testInstant.getEpochSecond();
			System.out.println ("is valid point for interpolation = " + function.isValidPoint(testDouble) ) ;
		} catch ( Exception e) {
			System.out.println("Exception -> " + e.getLocalizedMessage());
		}
	}
}