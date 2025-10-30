package folderDiscovery;

import org.junit.jupiter.api.Test;

import flights.FlightDataInterpolation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.logging.Logger;


public class TestFolderDiscovery {
	
	private static final Logger logger = Logger.getLogger(TestFolderDiscovery.class.getName());

	@Test
    public void testNumberOfFiles() {
		
		FolderDiscovery folderDiscovery = new FolderDiscovery();
		folderDiscovery.discover();
		
		//System.out.println("Files : " + folderDiscovery.getTrainFolderNbFiles());
        assertEquals( 11037 , folderDiscovery.getTrainFolderNbFiles());
        
		//System.out.println("Files: " + folderDiscovery.getRankFolderNbFiles());
		assertEquals( 1888 , folderDiscovery.getRankFolderNbFiles());
    }
	
	@Test
	public void browseTrainFiles() {
		
		FolderDiscovery folderDiscovery = new FolderDiscovery();
		folderDiscovery.discover();
		
		File[] files = folderDiscovery.getFlightTrainFiles();
		int count = 0;
		if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                	count = count + 1;
                    logger.info ("File: " + file.getName());
                    if ( count > 10) {
                    	break;
                    }
                }
            }
        } else {
            logger.info("Directory does not exist or is empty.");
        }
	}
}

