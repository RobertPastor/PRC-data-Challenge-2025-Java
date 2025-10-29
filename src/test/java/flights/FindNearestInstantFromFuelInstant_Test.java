package flights;

import java.io.IOException;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import fuel.FuelData;
import tech.tablesaw.api.Table;

public class FindNearestInstantFromFuelInstant_Test {

	@Test
    public void testNearestFlightTimStamp() throws IOException {

		FuelData fuelData = new FuelData(train_rank.rank );
		fuelData.readParquet();
		
		System.out.println("Row Count -> " + fuelData.getFuelDataTable().rowCount() );
		System.out.println("Shape -> " + fuelData.getFuelDataTable().shape() );
		System.out.println("Shape -> " + fuelData.getFuelDataTable().print(10) );
		
		//Instant fuelStart = fuelData.getFuelDataTable().column("timestamp").
				
		Instant now = Instant.now();
        System.out.println("Current Instant: " + now);

		FlightData flightData = new FlightData(train_rank.rank , "prc806615763");
		flightData.readParquet();
		
		System.out.println("Row Count -> " + flightData.getFlightDataTable().rowCount() );
		System.out.println("Shape -> " + flightData.getFlightDataTable().shape() );

		Instant nearestInstant = flightData.interpolateFromFuelStartEnd(now);
		System.out.println("nearest instant = " + now + " ---> " + nearestInstant);
		assert 	nearestInstant != null;
	}
}
