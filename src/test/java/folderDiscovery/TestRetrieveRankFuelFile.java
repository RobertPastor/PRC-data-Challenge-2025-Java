package folderDiscovery;

import java.io.File;

import org.junit.jupiter.api.Test;

import dataChallengeEnums.DataChallengeEnums.train_rank;

public class TestRetrieveRankFuelFile {

	@Test
    public void testReadFuelRankSubmissionFile() {
		
		FolderDiscovery folderDiscovery = new FolderDiscovery();
		folderDiscovery.discover();
		
		String fileName = "fuel_rank_submission.parquet";
		File file = folderDiscovery.getFuelFileFromFileName(train_rank.rank, fileName);
		assert file != null;
		assert file.exists();
		assert file.isFile();
		System.out.println(file.getAbsolutePath());
	}
}
