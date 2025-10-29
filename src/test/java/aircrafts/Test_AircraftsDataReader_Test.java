package aircrafts;

import java.io.IOException;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

public class Test_AircraftsDataReader_Test {
	
	private static final Logger logger = Logger.getLogger(Test_AircraftsDataReader_Test.class.getName());

	@Test
    public void testReadAircrafts () throws IOException {

		AircraftsData aircraftsDataReader = new AircraftsData();
		aircraftsDataReader.readExcelFile();
		
		logger.info("aircrafts database -> row count = " + aircraftsDataReader.getAircraftDataTable().rowCount() );
		assert ( aircraftsDataReader.getAircraftDataTable().rowCount() > 100);
		assert ( aircraftsDataReader.getAircraftDataTable().rowCount() < 400);
	}
}
