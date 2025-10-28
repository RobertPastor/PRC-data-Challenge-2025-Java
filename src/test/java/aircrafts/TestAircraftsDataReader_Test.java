package aircrafts;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class TestAircraftsDataReader_Test {

	@Test
    public void testReadAircrafts () throws IOException {

		AircraftsData aircraftsDataReader = new AircraftsData();
		aircraftsDataReader.readExcelFile();
		
		System.out.println("aircrafts database -> row count = " + aircraftsDataReader.getAircraftDataTable().rowCount() );
		assert ( aircraftsDataReader.getAircraftDataTable().rowCount() > 100);
		assert ( aircraftsDataReader.getAircraftDataTable().rowCount() < 400);
	}
}
