package fuelWithTableSplitter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.*;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

import aircrafts.AircraftsData;
import airports.AirportsData;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import flightLists.FlightListData;
import fuel.FuelData;
import tech.tablesaw.api.Row;

public class Test_FuelTableConcurrentAccess_Test {
	
	private static final Logger logger = Logger.getLogger(Test_FuelTableConcurrentAccess_Test.class.getName());

	@Test
	public void processParallelyWithExecutorService() throws InterruptedException, IOException {
		
		train_rank train_rank_value = train_rank.train;

		long maxToBeComputedRow = 1000000;
		//long maxToBeComputedRow = 100;

		FuelData fuelData = new FuelData(train_rank_value , maxToBeComputedRow);
		fuelData.readParquet();
		
		logger.info( fuelData.getFuelDataTable().shape());
		logger.info( fuelData.getFuelDataTable().structure().print());
		
		AirportsData airportsData = new AirportsData();
		airportsData.readParquet();

		AircraftsData aircraftsData = new AircraftsData();
		aircraftsData.readExcelFile();
		
		FlightListData flightListData = new FlightListData(train_rank_value);
		flightListData.readParquet();
		// convert the flight date into Year, Month and day of the year
		flightListData.extendWithFlightDateData();

		logger.info( flightListData.getFlightListDataTable().shape() );
		
		flightListData.extendWithAirportData( airportsData );
		flightListData.extendWithAirportsSinusCosinusOfLatitudeLongitude();
		
		logger.info(flightListData.getFlightListDataTable().shape());
		logger.info(flightListData.getFlightListDataTable().structure().print());
		
		flightListData.extendWithAircraftsData( aircraftsData );
		
		// extend fuel with end minus start differences
		fuelData.extendFuelWithEndStartDifference();
		
		logger.info("fuel data table - row count = " +  fuelData.getFuelDataTable().rowCount());
		
		// extend fuel with fuel flow in Kilograms per seconds
		fuelData.extendFuelFlowKgSeconds();
		
		// merge fuel with flight list
		fuelData.extendFuelWithFlightListData( flightListData.getFlightListDataTable() ) ;
		
		// extend fuel with flight data - there is a "merge" between fuel and flight data
		//fuelData.extendFuelStartEndInstantsWithFlightData();
		
		fuelData.createExtendedEngineeringFeatures();
		
		//============================================
		// exploiting multi threading for 32 cores some waiting for IO operations on parquet files
		//============================================
		
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of available processors: " + cores);
		// create a pool of thread to execute concurrently
		ExecutorService executor = Executors.newFixedThreadPool(2*cores);

		ConcurrentLinkedDeque<Integer> fuelTableIndexes = new ConcurrentLinkedDeque<Integer>();
		fuelData.getFuelDataTable().stream().forEach(row -> {
			// build a list of fuel indexes to share between thread
			logger.info("add index to the linked queue = " + String.valueOf(row.getRowNumber()));
			fuelTableIndexes.add(row.getRowNumber());
		});
		// start time
		LocalDateTime startTime = LocalDateTime.now();
		// start loop
		while (!fuelTableIndexes.isEmpty()) {
			int rowIndex = fuelTableIndexes.removeFirst();
			Row row = fuelData.getFuelDataTable().row(rowIndex);
			executor.execute( () -> {
				
				logger.info("start executor -> " + row.getRowNumber());
				fuelData.extendOneFuelRowStartEndInstantWithFlightData(startTime , row);
				
			});
		}
		
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		System.out.println("---- it is finished ----");
		System.out.println( fuelData.getFuelDataTable().shape());
		System.out.println( fuelData.getFuelDataTable().structure().print());
		System.out.println( fuelData.getFuelDataTable().print(10));

		
		// last step generate the parquet file
		fuelData.generateParquetFileFor();
		// generate a text file
		fuelData.generateListOfErrors();
	}
	
}
