package folderDiscovery;

import java.io.File;

import org.junit.jupiter.api.Test;

public class TestRetrieveRankFuelFile {

	
	@Test
    public void testNumberOfFiles() {
		
		FolderDiscovery folderDiscovery = new FolderDiscovery();
		folderDiscovery.discover();
		
		String fileName = "fuel_rank_submission.parquet";
		File file = folderDiscovery.getRankFuelFileFromFileName(fileName);
		assert file != null;
		assert file.exists();
		assert file.isFile();
		
	}
}
