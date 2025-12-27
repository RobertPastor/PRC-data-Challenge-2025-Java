package flights;

import java.io.IOException;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import fuel.FuelData;
import utils.CustomException;

public class Test_InterpolateFromFuelInstant_Test {

	@Test
    public void testNearestFlightTimeStamp() throws IOException, CustomException {
		
		FlightData flightData = new FlightData(train_rank_final.rank , "prc806615763");
		flightData.readParquetWithStream();
		
		FlightListData flightListData = new FlightListData(train_rank_final.rank, "A320");
		flightListData.readParquet();

		long nbMaxRecordsToRead = 100;
		FuelData fuelData = new FuelData(train_rank_final.rank , nbMaxRecordsToRead);
		fuelData.readParquet(flightListData);
		
		System.out.println("Row Count -> " + fuelData.getFuelDataTable().rowCount() );
		System.out.println("Shape -> " + fuelData.getFuelDataTable().shape() );
		System.out.println("Shape -> " + fuelData.getFuelDataTable().print(10) );
		
		//Instant fuelStart = fuelData.getFuelDataTable().column("timestamp").
				
		Instant now = Instant.now();
        System.out.println("Current Instant: " + now);

		//flightData.getFlightDataTable().buildInterpolationFunctions();
		
		//System.out.println("Row Count -> " + flightData.getFlightDataTable().rowCount() );
		//System.out.println("Shape -> " + flightData.getFlightDataTable().shape() );

		//Instant nearestInstant = flightData.interpolateFromFuelStartEnd(now);
		//System.out.println("nearest instant = " + now + " ---> " + nearestInstant);
		//assert 	nearestInstant != null;
	}
}
