package flights;

import java.io.IOException;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.Table;

public class Test_Summarize_Test {

	@Test
	public void testTableSummarize () throws IOException {		

		System.out.println("--------- start test summarize -----");

		double[] numbers = {1, 2, 3, 4};

		DoubleColumn nc = DoubleColumn.create("nc", numbers);
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

		assert ( table.columnNames().contains("xt") );

	}
}
