package fuel;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import flights.FlightData;

public class TestColumnInterpolation {

	@Test
    public void testReadExtendFuelTrain() throws IOException {
		
		train_rank train_rank_value = train_rank.train;
		String flight_id = "prc770822360";
		
		FlightData flightData = new FlightData( train_rank_value , flight_id );
		flightData.readParquetWithStream();
		
		System.out.println( flightData.getFlightDataTable().print(20));
		System.out.println( flightData.getFlightDataTable().doubleColumn("altitude").countMissing());
		
		List<String> columnToInterpolatelist = List.of("latitude","longitude", "altitude","groundspeed", "track", "vertical_rate","mach", "TAS", "CAS");

		flightData.generatedInterpolationFunction( columnToInterpolatelist ) ;

		//String interpolatedColumnName = "interpolated_" + "altitude" + "_" + "start";

		
	}
}
