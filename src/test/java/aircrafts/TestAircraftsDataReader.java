package aircrafts;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class TestAircraftsDataReader {

	@Test
    public void testReadAircrafts () throws IOException {

		AircraftsDataReader aircraftsDataReader = new AircraftsDataReader();
		aircraftsDataReader.readExcelFile();
	}
}
