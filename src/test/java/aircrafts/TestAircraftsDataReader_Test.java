package aircrafts;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class TestAircraftsDataReader_Test {

	@Test
    public void testReadAircrafts () throws IOException {

		AircraftsData aircraftsDataReader = new AircraftsData();
		aircraftsDataReader.readExcelFile();
	}
}
