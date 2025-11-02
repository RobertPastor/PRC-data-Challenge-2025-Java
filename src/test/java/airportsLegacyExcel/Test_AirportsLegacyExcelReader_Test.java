package airportsLegacyExcel;

import java.io.IOException;

import org.junit.jupiter.api.Test;


public class Test_AirportsLegacyExcelReader_Test {
	
	@Test
    public void testReadLegacyEXCELairports () throws IOException {

		AirportsLegacyExcelData airportsData = new AirportsLegacyExcelData();
		airportsData.readExcelFile();
	}

}
