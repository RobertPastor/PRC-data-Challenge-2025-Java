package aircrafts;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.dhatim.fastexcel.reader.Cell;
import org.dhatim.fastexcel.reader.ReadableWorkbook;
import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.Sheet;

import tech.tablesaw.io.xlsx.XlsxReadOptions;
import tech.tablesaw.io.xlsx.XlsxReader;


public class AircraftsDataReader extends AircraftsDataTable {
	
    private static final Logger logger = Logger.getLogger(AircraftsDataReader.class.getName());

	private static String aircraftsFileName = "FAA-Aircraft-Char-DB-AC-150-5300-13B-App-2023-09-07.xlsx";
	
	public static String getAircraftsFileName() {
		return aircraftsFileName;
	}

	AircraftsDataReader() {
		logger.info("file = " + getAircraftsFileName());
	}
	
	public void readTableSawExcelFile() throws IOException {
		
		InputStream in = AircraftsDataReader.class.getResourceAsStream(AircraftsDataReader.aircraftsFileName);
		logger.info(AircraftsDataReader.getAircraftsFileName());
		
		this.createEmptyAircraftsDataTable();
		this.aircraftsDataTable = new XlsxReader()
		            .read(XlsxReadOptions.builder(in)
		            .sheetIndex(0)
		            .build());
		logger.info(this.aircraftsDataTable.print(10));
	}
	
	public void readExcelFile() throws IOException {
		
		this.createEmptyAircraftsDataTable();
		
		String sheetName = "ACD_Data";

		// java.net.URL
		InputStream in = AircraftsDataReader.class.getResourceAsStream(AircraftsDataReader.aircraftsFileName);
		logger.info(AircraftsDataReader.getAircraftsFileName());
		System.out.println(AircraftsDataReader.getAircraftsFileName());
		
		if (in != null) {
			System.out.println(in.available());
			try (ReadableWorkbook wb = new ReadableWorkbook(in)) {
	            Optional<Sheet> sheet = wb.findSheet(sheetName);
	            if (sheet.isPresent()) {
	            	Sheet foundSheet = sheet.get();
		            try (Stream<Row> rows = foundSheet.openStream()) {
		                rows.forEach(r -> {
		                	logger.info( String.valueOf( r.getRowNum() ) );
		                	//this.appendRowToAircraftsDataTable(r);
		
		                    for (Cell cell : r) {
		                        logger.info( cell.getRawValue() );
		                    }
		                });
		            }
	            }
	        }
		} else {
			System.out.println("file not found");
		}
	}
}
