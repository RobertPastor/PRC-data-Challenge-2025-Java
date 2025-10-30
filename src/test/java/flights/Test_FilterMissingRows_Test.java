package flights;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.selection.Selection;

public class Test_FilterMissingRows_Test {

	@Test
	public void testTableFilterMissingValues () throws IOException {		

		System.out.println("--------- start test filter missing values -----");
		
		double[] numbers = {1, 2, 3, 4};
		int arraySize = numbers.length;
		ArrayList<Double> arrayListWithNulls = new ArrayList<>();
		for (int i = 0; i< arraySize; i++) {
			if ( i == 2 ) {
				arrayListWithNulls.add(null);
			} else {
				arrayListWithNulls.add(numbers[i]);
			}
		}
		System.out.println( arrayListWithNulls );
		Double[] numbersWithNulls = new Double[arraySize];
		
		for ( int i = 0 ; i < arraySize ; i++ ) {
			numbersWithNulls[i] = (Double)arrayListWithNulls.get(i);
		}
		System.out.println ( numbersWithNulls );

		DoubleColumn nc = DoubleColumn.create("nc", numbersWithNulls);
		System.out.println(nc.print());

		Instant instant1 = Instant.parse("2020-04-09T10:15:30.00Z");
		System.out.println("Instant1: " + instant1);

		Instant instant2 = Instant.parse("2025-04-09T10:15:32.00Z");
		System.out.println("Instant2: " + instant2);

		Instant instant3 = Instant.parse("2025-04-10T10:15:32.00Z");
		System.out.println("Instant3: " + instant3);

		Instant instant4 = Instant.parse("2025-09-10T10:15:32.00Z");
		System.out.println("Instant4: " + instant4);
		
		Instant[] xInstants = new Instant[] { instant1 , instant2 , instant3 , instant4 };

		InstantColumn xt = InstantColumn.create("xt" , xInstants);
		System.out.println(xt.print());

		Table table = Table.create("test");
		table.addColumns(xt);
		table.addColumns(nc);

		assert ( table.columnNames().contains("xt") );
		assert ( table.columnNames().contains("nc") );

		System.out.println( table.print());
		
		Selection selectionNotMissing = table.doubleColumn("nc").isNotMissing();
		Table filteredTable = table.where(selectionNotMissing);
		
		System.out.println( filteredTable.print());
		
		System.out.println("size of table with nulls = " + table.rowCount() + " ---> size of table without missing values = " + filteredTable.rowCount());
		
		assert table.rowCount() == filteredTable.rowCount() + 1;
	}
}
