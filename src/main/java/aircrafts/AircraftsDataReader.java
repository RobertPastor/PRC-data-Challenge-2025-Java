package aircrafts;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.dhatim.fastexcel.reader.Cell;
import org.dhatim.fastexcel.reader.ReadableWorkbook;
import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.Sheet;



public class AircraftsDataReader extends AircraftsDataTable {
	
    private static final Logger logger = Logger.getLogger(AircraftsDataReader.class.getName());

	private static String aircraftsFileName = "FAA-Aircraft-Char-DB-AC-150-5300-13B-App-2023-09-07.xlsx";
	
	public static String getAircraftsFileName() {
		return aircraftsFileName;
	}

	AircraftsDataReader() {
		logger.info("file = " + getAircraftsFileName());
	}
	
	public void readExcelFile() throws IOException {
		
		this.createEmptyAircraftsDataTable();
		logger.info(AircraftsDataReader.getAircraftsFileName());
		
		String sheetName = "ACD_Data";
		
		String fileName = AircraftsDataReader.getAircraftsFileName();
		Path path = Paths.get("C:/Users/rober/eclipse-2025-09/eclipse-jee-2025-09-R-win32-x86_64/Data-Challenge-2025/documents" , fileName);
		File inputExcelFile = path.toFile();
		
		if ( inputExcelFile.exists() && inputExcelFile.isFile()) {
		
			System.out.println(inputExcelFile.getAbsolutePath());
			
			try (ReadableWorkbook wb = new ReadableWorkbook(inputExcelFile)) {
				
				// Access the defined names in the workbook
	            //Optional<DefinedName> definedName = workbook.getDefinedNames()
	            //                                            .stream()
	            //                                            .filter(name -> name.getName().equals("ICAO_Code"))
	            //                                            .findFirst();
				
	            Optional<Sheet> sheet = wb.findSheet(sheetName);
	            if (sheet.isPresent()) {
	            	Sheet foundSheet = sheet.get();
	            	
		            try (Stream<Row> rows = foundSheet.openStream()) {
		                rows.forEach(r -> {
		                	
		                	// assumption - row with row num 0 contains the header
		                	if ( r.getRowNum() == 0) {
		                		
		                	}
		                	
		                	logger.info( String.valueOf( r.getRowNum() ) );
		                	//this.appendRowToAircraftsDataTable(r);
		
		                    for (Cell cell : r) {
		                    	if (cell != null) {
		                    		System.out.println(cell.getAddress().toString());
                                    //System.out.println("column index = " + cell.getColumnIndex() + " cell type = " + cell.getType());
                                    //System.out.println( " -> " + r.getRowNum() + " -> " + cell.getRawValue());
                                    //data.get(r.getRowNum()).add(cell.getRawValue());
		                    	}
		                        //logger.info( cell.getRawValue() );
		                    }
		                });
		                System.out.println(rows.count());
		            } catch ( Exception e) {
                        System.out.println(e.getLocalizedMessage());
		            }
	            }
	        } catch ( Exception e) {
                System.out.println(e.getLocalizedMessage());
            }
		} else {
			System.out.println("file <<" + inputExcelFile.getAbsolutePath() + ">> not found or it is not a file");
		}
	}
}
