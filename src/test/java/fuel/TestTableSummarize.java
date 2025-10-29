package fuel;


import tech.tablesaw.api.*;
import tech.tablesaw.aggregate.AggregateFunctions;

import java.io.IOException;
import java.time.Instant;

import org.junit.jupiter.api.Test;



public class TestTableSummarize {
	
	@Test
    public void testSummarize_two() throws IOException {
		
   
        // Create a table with an Instant column
        Table table = Table.create("Example Table")
                .addColumns(
                        InstantColumn.create("Timestamp", 
                        		Instant.now(), 
                        		Instant.now(),
                        		Instant.now().minusSeconds(3600), 
                        		Instant.now().minusSeconds(7200)),
                        DoubleColumn.create("Value", 10.55, 10.75, 20.0, 30.0)
                );
	
     // Group by the Instant column (or transform it to a higher-level unit like date)
        Table summarized = table.summarize("Value", 
        		AggregateFunctions.first, AggregateFunctions.sum, AggregateFunctions.median)
                .by("timestamp");

        // Print the summarized table
        System.out.println("first -> " + summarized);
        
	}
	@Test
    public void testSummarize_One() throws IOException {
		
   
        // Create a table with an Instant column
        Table table = Table.create("Example Table")
                .addColumns(
                        InstantColumn.create("Timestamp", Instant.now(), 
                        		Instant.now().minusSeconds(3600), 
                        		Instant.now().minusSeconds(7200),
                        		Instant.now().minusSeconds(8200)),
                        DoubleColumn.create("Value", 7.5, 10.5, 20.0, 30.0)
                );

        // Group by the Instant column (or transform it to a higher-level unit like date)
        Table summarized = table.summarize("Value", AggregateFunctions.mean, AggregateFunctions.sum)
                .by("Timestamp");

        // Print the summarized table
        System.out.println(summarized);
    }
}

