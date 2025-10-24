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

import utils.Constants;



public class AircraftsData extends AircraftsDataTable {

	public class HeaderException extends RuntimeException {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public HeaderException(String message) {
			super(message);
		}
	}

	private static final Logger logger = Logger.getLogger(AircraftsData.class.getName());

	private static String aircraftsFileName = "FAA-Aircraft-Char-DB-AC-150-5300-13B-App-2023-09-07.xlsx";

	private static String aircraftsFolderPath = "C:/Users/rober/eclipse-2025-09/eclipse-jee-2025-09-R-win32-x86_64/Data-Challenge-2025/documents";

	private static List<String> aircraftsExpectedHeaders = Arrays.asList("ICAO_Code","Num_Engines","Approach_Speed_knot","Wingspan_ft_without_winglets_sharklets",
			"Length_ft" , "Tail_Height_at_OEW_ft" , "Wheelbase_ft" ,"Cockpit_to_Main_Gear_ft" , "Main_Gear_Width_ft" , "MTOW_lb","MALW_lb", "Parking_Area_ft2");

	private static List<Integer> aircraftsFoundHeadersColumnIndexes = new ArrayList<Integer>();
	private static List<String> aircraftsFoundHeaders = new ArrayList<String>();

	public static List<String> getAircraftsFoundHeaders() {
		return aircraftsFoundHeaders;
	}

	public static List<Integer> getAircraftHeadersColumnIndexes() {
		return aircraftsFoundHeadersColumnIndexes;
	}

	public static List<String> getAircraftsExpectedHeaders() {
		return aircraftsExpectedHeaders;
	}

	public static String getAircraftsFileName() {
		return aircraftsFileName;
	}

	public AircraftsData() {
		logger.info("file = " + getAircraftsFileName());
	}
	
	private void buildHeadersInformations ( final Row r ) {
		for (Cell cell : r) {
			if (cell != null) {
				System.out.println("column index = " + cell.getColumnIndex() + " cell type = " + cell.getType());
				if ( cell.getType().equals(CellType.STRING)) {
					String headerNameFound =  cell.getRawValue();
					System.out.println("header found = " + headerNameFound );
					if ( AircraftsData.getAircraftsExpectedHeaders().contains(headerNameFound)) {
						AircraftsData.getAircraftsFoundHeaders().add(headerNameFound);
						AircraftsData.getAircraftHeadersColumnIndexes().add(cell.getColumnIndex());
					}
				}
			}
		}
		System.out.println(AircraftsData.getAircraftsExpectedHeaders());
		System.out.println(AircraftsData.getAircraftsFoundHeaders());
		System.out.println(AircraftsData.getAircraftHeadersColumnIndexes());

	}
	
	private void fillTableRow(final Row r) {
		
		tech.tablesaw.api.Row tableRow = this.aircraftsDataTable.appendRow();
		logger.info ("---> " + this.aircraftsDataTable.rowCount());

		//ICAO_Code
		int columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(0);
		Cell cell = r.getCell(columnIndex);
		tableRow.setString("ICAO_Code", cell.getRawValue());

		//Num_Engines
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(1);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			java.math.BigDecimal bigDecimal = (java.math.BigDecimal)cell.getValue();
			long longValue = bigDecimal.longValueExact();
			int intValue = Math.toIntExact(longValue);
			tableRow.setInt("Num_Engines", intValue );
		}

		//Approach_Speed_knot
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(2);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Approach_Speed_knot", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//Wingspan_ft_without_winglets_sharklets
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(3);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Wingspan_ft_without_winglets_sharklets", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//Length_ft
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(4);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Length_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//Tail_Height_at_OEW_ft
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(5);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Tail_Height_at_OEW_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//Wheelbase_ft
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(6);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Wheelbase_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//float Cockpit_to_Main_Gear_ft,
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(7);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Cockpit_to_Main_Gear_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//float Main_Gear_Width_ft,
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(8);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Main_Gear_Width_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		// MTOW_lb Maximum take-off weight
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(9);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			double doubleValue = (double) utils.Utils.getDoubleFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) ;
			tableRow.setDouble("MTOW_lb", doubleValue);
			tableRow.setDouble("MTOW_kg", doubleValue * Constants.lbs_to_kilograms);
		}

		//double MALW_lb,						
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(10);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			double doubleValue = (double) utils.Utils.getDoubleFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) ;
			tableRow.setDouble("MALW_lb", doubleValue );
			tableRow.setDouble("MALW_kg", doubleValue * Constants.lbs_to_kilograms);
		}

		//float Parking_Area_ft2
		columnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(11);
		cell = r.getCell(columnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Parking_Area_ft2", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}
		
		
	}

	public void readExcelFile() throws IOException {

		this.createEmptyAircraftsDataTable();
		logger.info(this.aircraftsDataTable.structure().print());
		
		logger.info(AircraftsData.getAircraftsFileName());

		String sheetName = "ACD_Data";

		String fileName = AircraftsData.getAircraftsFileName();
		Path path = Paths.get( AircraftsData.aircraftsFolderPath, fileName);

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
						this.buildHeadersInformations(r);
					} else {
						//logger.info( String.valueOf( r.getRowNum() ) );
						this.fillTableRow(r);
					}
				}
				System.out.println( this.aircraftsDataTable.structure() );
				System.out.println( this.aircraftsDataTable.print(10) );
			} 
			wb.close();
		} else {
			System.out.println("file <<" + inputExcelFile.getAbsolutePath() + ">> not found or it is not a file");
		}
	}
}
