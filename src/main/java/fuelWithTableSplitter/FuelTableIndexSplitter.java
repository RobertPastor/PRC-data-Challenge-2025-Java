package fuelWithTableSplitter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FuelTableIndexSplitter {

	public int getAvailableProcessors() {
		int nbProcessors = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available processors: " + nbProcessors);
		return nbProcessors;
	}
	
	public void splitFuelTableAndLaunchExecutors( final int fuelTableRowCount , final int nbProcessors) {
		
		int totalRows = fuelTableRowCount; // Total number of rows in the table
        int threadCount = nbProcessors;  // Number of threads to use

        // Calculate the range size for each thread
        int rangeSize = totalRows / threadCount;

        // Create a thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            int start = i * rangeSize;
            int end = (i == threadCount - 1) ? totalRows : start + rangeSize;

            // Submit a task for each range
            //executor.submit(new TableProcessor(start, end));
        }

        // Shutdown the executor
        executor.shutdown();
		
	}
}
