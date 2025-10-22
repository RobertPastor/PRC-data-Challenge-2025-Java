package aircrafts;

import java.io.IOException;

public class Test_AircraftsDatabaseReader_With_Main {

	public static void main(String[] args) throws IOException {
		
		AircraftsDataReader aircraftsDataReader = new AircraftsDataReader();
		aircraftsDataReader.readExcelFile();

	}

}
