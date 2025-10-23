package aircrafts;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import org.dhatim.fastexcel.reader.Cell;
import org.dhatim.fastexcel.reader.CellType;
import org.dhatim.fastexcel.reader.ReadableWorkbook;
import org.dhatim.fastexcel.reader.Row;
import org.dhatim.fastexcel.reader.Sheet;


public class AircraftsDataReader extends AircraftsDataTable {

	public class HeaderException extends RuntimeException {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public HeaderException(String message) {
			super(message);
		}
	}

	private static final Logger logger = Logger.getLogger(AircraftsDataReader.class.getName());

	private static String aircraftsFileName = "FAA-Aircraft-Char-DB-AC-150-5300-13B-App-2023-09-07.xlsx";
	private static String aircraftsFolderPath = "C:/Users/rober/eclipse-2025-09/eclipse-jee-2025-09-R-win32-x86_64/Data-Challenge-2025/documents";

	private static List<String> aircraftsHeaders = Arrays.asList("ICAO_Code","Num_Engines","Num_Engines","Approach_Speed_knot","Wingspan_ft_without_winglets_sharklets",
			"Length_ft" , "Tail_Height_at_OEW_ft" , "Wheelbase_ft" ,"Cockpit_to_Main_Gear_ft" , "Main_Gear_Width_ft" , "MTOW_lb","MTOW_lb", "Parking_Area_ft2");

	private static List<Integer> aircraftHeadersColumnIndexex = new ArrayList<Integer>();

	public static List<Integer> getAircraftHeadersColumnIndexes() {
		return aircraftHeadersColumnIndexex;
	}

	public static List<String> getAircraftsHeaders() {
		return aircraftsHeaders;
	}

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
		Path path = Paths.get( AircraftsDataReader.aircraftsFolderPath, fileName);

		File inputExcelFile = path.toFile();

		if ( inputExcelFile.exists() && inputExcelFile.isFile()) {

			System.out.println(inputExcelFile.getAbsolutePath());

			ReadableWorkbook wb = new ReadableWorkbook(inputExcelFile);

			// Access the defined names in the workbook
			//Optional<DefinedName> definedName = workbook.getDefinedNames()
			//                                            .stream()
			//                                            .filter(name -> name.getName().equals("ICAO_Code"))
			//                                            .findFirst();

			Optional<Sheet> sheet = wb.findSheet(sheetName);
			if (sheet.isPresent()) {
				Sheet foundSheet = sheet.get();

				List<Row> listOfRows = foundSheet.read();

				Iterator<Row> iter = listOfRows.iterator();
				while (iter.hasNext()) {
					Row r = iter.next();

					// assumption - row with row number 1 contains the header
					if ( r.getRowNum() == 1) {
						for (Cell cell : r) {
							if (cell != null) {
								System.out.println("column index = " + cell.getColumnIndex() + " cell type = " + cell.getType());
								if ( cell.getType().equals(CellType.STRING)) {
									String headerNameFound =  cell.getRawValue();
									System.out.println("header found = " + headerNameFound );
									if ( AircraftsDataReader.getAircraftsHeaders().contains(headerNameFound)) {
										AircraftsDataReader.getAircraftHeadersColumnIndexes().add(cell.getColumnIndex());
									}
								}
							}
						}
						System.out.println(AircraftsDataReader.getAircraftsHeaders());
						System.out.println(AircraftsDataReader.getAircraftHeadersColumnIndexes());

					} else {
						//logger.info( String.valueOf( r.getRowNum() ) );
						//this.appendRowToAircraftsDataTable(r);

						tech.tablesaw.api.Row tableRow = this.aircraftsDataTable.appendRow();

						int columnIndex = AircraftsDataReader.getAircraftHeadersColumnIndexes().get(0);
						Cell cell = r.getCell(columnIndex);
						tableRow.setString("ICAO_Code", cell.getRawValue());
						
						columnIndex = AircraftsDataReader.getAircraftHeadersColumnIndexes().get(1);
						cell = r.getCell(columnIndex);
						tableRow.setInt("Num_Engines", (Integer)cell.getValue());
						
						columnIndex = AircraftsDataReader.getAircraftHeadersColumnIndexes().get(2);
						cell = r.getCell(columnIndex);
						tableRow.setFloat("Approach_Speed_knot", (float)cell.getValue());



						/*
								for (Cell cell : r) {
									if (cell != null) {
										//System.out.println(cell.getAddress().toString());
										if ( AircraftsDataReader.getAircraftHeadersColumnIndexes().contains(cell.getColumnIndex())) {
											//String columnName = AircraftsDataReader.getAircraftsHeaders().get(0);
											//tableRow.

										}
										//System.out.println("column index = " + cell.getColumnIndex() + " cell type = " + cell.getType());
										//System.out.println( " -> " + r.getRowNum() + " -> " + cell.getRawValue());
										//data.get(r.getRowNum()).add(cell.getRawValue());
									}
									//logger.info( cell.getRawValue() );
								}
						 */
					}
				}
				System.out.println( this.aircraftsDataTable.print(10) );
			} 
			wb.close();
		} else {
			System.out.println("file <<" + inputExcelFile.getAbsolutePath() + ">> not found or it is not a file");
		}
	}
}
