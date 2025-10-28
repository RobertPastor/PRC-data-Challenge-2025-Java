package fuel;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestExtendFuelDataWithNearestFlightData_Rank {

	@Test
	public void testReadExtendFuelRank() throws IOException {

		train_rank train_rank_value = train_rank.rank;

		long maxToBeComputedRow = 1000000;
		//int maxToBeComputedRow = 100;

		CommonToRankAndTrain.CommonToTrainAndRank(train_rank_value, maxToBeComputedRow);

	}
}
