package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestExtendFuelDataWithNearestFlightData_Train {

		@Test
	    public void testReadExtendFuelTrain() throws IOException {
			
			train_rank train_rank_value = train_rank.train;
			
			long maxToBeComputedRow = 1000000;
			maxToBeComputedRow = 100;
			
			CommonToRankAndTrain.CommonToTrainAndRank(train_rank_value, maxToBeComputedRow);
			
		}
}
