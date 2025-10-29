package aircrafts;

import java.io.IOException;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

public class Test_AircraftIsExistingInDatabase_Test {
	
	private static final Logger logger = Logger.getLogger(Test_AircraftsDataReader_Test.class.getName());

	@Test
    public void testReadAircrafts () throws IOException {		
		AircraftsData aircraftsDataReader = new AircraftsData();
		aircraftsDataReader.readExcelFile();

		String aircraftICAOcode = "A20N";
		boolean yes_no = aircraftsDataReader.isAircraftICAOcodeInDatabase(aircraftICAOcode);
		logger.info("is ICAO code <<" + aircraftICAOcode + ">> in database = " + String.valueOf(yes_no));
	}

}
