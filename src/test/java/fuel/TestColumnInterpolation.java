package fuel;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;
import flightLists.FlightListData;
import flights.FlightData;
import utils.CustomException;

public class TestColumnInterpolation {

	@Test
    public void testReadExtendFuelTrain() throws IOException, CustomException {
		
		train_rank_final train_rank_value = train_rank_final.train;
		String flight_id = "prc770822360";
		
		FlightListData flightListData = new FlightListData(train_rank_value);
		flightListData.readParquet();
		
		Instant takeoff = flightListData.getTakeoffInstant(flight_id);
		
		FlightData flightData = new FlightData( train_rank_value , flight_id );
		flightData.readParquetWithStream();
		
		System.out.println( flightData.getFlightDataTable().print(20));
		System.out.println( flightData.getFlightDataTable().doubleColumn("altitude").countMissing());
		
		//double altitude = flightData.getDoubleFlightDataAtInterpolatedStartEndFuelInstant("altitude", takeoff);
		
		//System.out.println("altitude at takeoff = " + altitude );

	}
}
