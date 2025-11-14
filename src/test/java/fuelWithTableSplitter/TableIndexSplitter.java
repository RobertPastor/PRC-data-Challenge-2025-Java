package fuelWithTableSplitter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import fuel.FuelData;

//Task to process a specific range of rows
class TableProcessor implements Runnable {
	private final int startIndex;
	private final int endIndex;

	public TableProcessor(int startIndex, int endIndex) {
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		System.out.println(" start index = " + startIndex + " --> end index = "+ endIndex);
	}

	@Override
	public void run() {
		System.out.println("Processing rows from " + startIndex + " to " + endIndex);
		// Add your table processing logic here
	}
}


public class TableIndexSplitter {

	public static void main(String[] args) throws IOException {

		train_rank_final train_rank_value = train_rank_final.train;
		int maxToBeAnalysedRowCount = 1000000;
		
		FuelDataSplitter fuelDataSplitter = new FuelDataSplitter(train_rank_value , maxToBeAnalysedRowCount);
		fuelDataSplitter.readParquet();
		
		System.out.println("fuel table -> row Count = " + fuelDataSplitter.getFuelDataTable().rowCount());
		int totalRows = fuelDataSplitter.getFuelDataTable().rowCount(); // Total number of rows in the table
		
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available processors: " + cores);

		// Calculate the range size for each thread
		int threadCount = cores;  // Number of threads to use
		int rangeSize = totalRows / threadCount;
		System.out.println("range size = " + rangeSize);

		// Create a thread pool
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		// distribute work over the 16 threads
		for (int i = 0; i < threadCount; i++) {
			int start = i * rangeSize;
			int end = (i == threadCount - 1) ? totalRows : start + rangeSize;

			// Submit a task for each range
			executor.submit(new TableProcessor(start, end));
		}
		// Shutdown the executor
		executor.shutdown();
	}
}
