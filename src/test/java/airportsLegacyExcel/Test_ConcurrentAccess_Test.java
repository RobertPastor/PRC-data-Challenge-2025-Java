package airportsLegacyExcel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import tech.tablesaw.api.Row;

public class Test_ConcurrentAccess_Test {

	@Test
	public void processParallelyWithExecutorService() throws InterruptedException, IOException {

		AirportsLegacyExcelData airportsLegacyExcelData = new AirportsLegacyExcelData();
		airportsLegacyExcelData.readExcelFile(); 

		int numberOfRows = airportsLegacyExcelData.getAirportsDataTable().rowCount();

		System.out.println( numberOfRows );

		System.out.println( airportsLegacyExcelData.getAirportsDataTable().shape());
		System.out.println( airportsLegacyExcelData.getAirportsDataTable().print(10));

		airportsLegacyExcelData.createLatitudeLongitudeAsRadiansColumns ();
		System.out.println( airportsLegacyExcelData.getAirportsDataTable().shape());

		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available processors: " + cores);

		ExecutorService executor = Executors.newFixedThreadPool(cores);

		ConcurrentLinkedDeque<Integer> airportsTableIndexes = new ConcurrentLinkedDeque<Integer>();
		airportsLegacyExcelData.getAirportsDataTable().stream()
		.forEach(row -> {
			airportsTableIndexes.add(row.getRowNumber());
		});

		while (!airportsTableIndexes.isEmpty()) {
			int rowIndex = airportsTableIndexes.removeFirst();
			Row row = airportsLegacyExcelData.getAirportsDataTable().row(rowIndex);
			executor.execute( () -> {
				airportsLegacyExcelData.extendOneRowWithLatitudeLongitudeRadians(row);
			});
		}
		
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		System.out.println("---- it is finished ----");
		System.out.println( airportsLegacyExcelData.getAirportsDataTable().shape());
		System.out.println( airportsLegacyExcelData.getAirportsDataTable().print(10));
	}
}
