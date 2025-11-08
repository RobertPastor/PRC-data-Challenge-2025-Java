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

import folderDiscovery.FolderDiscovery;
import utils.Constants;

public class AircraftsData extends AircraftsDataTable {

	public class HeaderException extends RuntimeException {
		/**
		 * exception raised when a Header is not in the expected headers list
		 */
		private static final long serialVersionUID = 1L;
		public HeaderException(String message) {
			super(message);
		}
	}

	private static final Logger logger = Logger.getLogger(AircraftsData.class.getName());

	private static String aircraftsFileName = "FAA-Aircraft-Char-DB-AC-150-5300-13B-App-2023-09-07.xlsx";

	private static List<String> aircraftsExpectedHeaders = Arrays.asList("ICAO_Code","Num_Engines","Approach_Speed_knot",
			"Wingspan_ft_without_winglets_sharklets","Wingspan_ft_with_winglets_sharklets",
			"Length_ft" , "Tail_Height_at_OEW_ft" , "Wheelbase_ft" ,"Cockpit_to_Main_Gear_ft" , 
			"Main_Gear_Width_ft" , "MTOW_lb","MALW_lb", "Parking_Area_ft2");

	private static List<Integer> aircraftsFoundHeadersColumnIndexes = new ArrayList<Integer>();
	private static List<String> aircraftsFoundHeaders = new ArrayList<String>();

	public AircraftsData() {
		super();
		logger.info("file = " + AircraftsData.getAircraftsFileName());
		logger.info("folder path = " + FolderDiscovery.getAircraftsFolderPath());
	}
	/**
	 * build the headers while analysing the first row from the EXCEL file
	 * @param r the row
	 */
	private void buildHeadersInformations ( final Row r ) {
		for (Cell cell : r) {
			if (cell != null) {
				logger.info("column index = " + cell.getColumnIndex() + " cell type = " + cell.getType());
				if ( cell.getType().equals(CellType.STRING)) {
					String headerNameFound =  cell.getRawValue();
					logger.info("header name found = " + headerNameFound );
					if ( AircraftsData.getAircraftsExpectedHeaders().contains(headerNameFound)) {
						// header column is in the list of expected headers
						AircraftsData.getAircraftsFoundHeaders().add(headerNameFound);
						AircraftsData.getAircraftHeadersColumnIndexes().add(cell.getColumnIndex());
					}
				}
			}
		}
		logger.info(AircraftsData.getAircraftsExpectedHeaders().toString());
		logger.info(AircraftsData.getAircraftsFoundHeaders().toString());
		logger.info(AircraftsData.getAircraftHeadersColumnIndexes().toString());
	}

	private void fillTableRow(final Row row) {

		tech.tablesaw.api.Row tableRow = this.aircraftsDataTable.appendRow();
		logger.info ("row count ---> " + this.aircraftsDataTable.rowCount());
		
		int columnIndex = 0;

		//ICAO_Code
		int headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		Cell cell = row.getCell(headerColumnIndex);
		tableRow.setString("ICAO_Code", cell.getRawValue());
		// 7th November 2025 - keep a copy of the aircraft_ICAO_code for further get dummies '''
		tableRow.setString("aircraft_ICAO_Code", cell.getRawValue());
		//logger.info("Aircraft ICAO code = " + cell.getRawValue());

		//Num_Engines
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			java.math.BigDecimal bigDecimal = (java.math.BigDecimal)cell.getValue();
			long longValue = bigDecimal.longValueExact();
			int intValue = Math.toIntExact(longValue);
			tableRow.setInt("Num_Engines", intValue );
		}

		//Approach_Speed_knot
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Approach_Speed_knot", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//Wingspan_ft_without_winglets_sharklets
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell != null) {
			if ( cell.getType().equals(CellType.NUMBER) ) {
				tableRow.setFloat("Wingspan_ft_without_winglets_sharklets", 
						(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
			}
		} else {
			tableRow.setFloat("Wingspan_ft_without_winglets_sharklets", (0.0f));
		}

		//Wingspan_ft_with_winglets_sharklets
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell != null) {
			if ( cell.getType().equals(CellType.NUMBER) ) {
				tableRow.setFloat("Wingspan_ft_with_winglets_sharklets", 
						(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
			}
		} else {
			tableRow.setFloat("Wingspan_ft_without_winglets_sharklets", (0.0f));
		}

		//Length_ft
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.NUMBER) ) {
				tableRow.setFloat("Length_ft", 
						(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
			}
		} else {
			tableRow.setFloat("Length_ft", (0.0f));
		}

		//Tail_Height_at_OEW_ft
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Tail_Height_at_OEW_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//Wheelbase_ft
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Wheelbase_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//float Cockpit_to_Main_Gear_ft,
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Cockpit_to_Main_Gear_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		//float Main_Gear_Width_ft,
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Main_Gear_Width_ft", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}

		// MTOW_lb Maximum take-off weight
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			double doubleValue = (double) utils.Utils.getDoubleFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) ;
			tableRow.setDouble("MTOW_lb", doubleValue);
			tableRow.setDouble("MTOW_kg", doubleValue * Constants.lbs_to_kilograms);
		}

		//double MALW_lb,						
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			double doubleValue = (double) utils.Utils.getDoubleFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) ;
			tableRow.setDouble("MALW_lb", doubleValue );
			tableRow.setDouble("MALW_kg", doubleValue * Constants.lbs_to_kilograms);
		}

		//float Parking_Area_ft2
		headerColumnIndex = AircraftsData.getAircraftHeadersColumnIndexes().get(columnIndex++);
		cell = row.getCell(headerColumnIndex);
		if ( cell.getType().equals(CellType.NUMBER) ) {
			tableRow.setFloat("Parking_Area_ft2", 
					(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
		}
	}

	/**
	 * read an EXCEL file using fastExcel library
	 * @throws IOException
	 */
	public void readExcelFile() throws IOException {

		this.createEmptyAircraftsDataTable();
		logger.info(this.aircraftsDataTable.structure().print());

		logger.info(AircraftsData.getAircraftsFileName());
		// name of the sheet in the EXCEL file
		String sheetName = "ACD_Data";

		String fileName = AircraftsData.getAircraftsFileName();
		Path path = Paths.get( FolderDiscovery.getAircraftsFolderPath() , fileName);

		File inputExcelFile = path.toFile();

		if ( inputExcelFile.exists() && inputExcelFile.isFile()) {

			logger.info(inputExcelFile.getAbsolutePath());
			ReadableWorkbook wb = new ReadableWorkbook(inputExcelFile);
			Optional<Sheet> sheet = wb.findSheet(sheetName);
			if (sheet.isPresent()) {
				Sheet foundSheet = sheet.get();

				List<Row> listOfRows = foundSheet.read();

				Iterator<Row> iter = listOfRows.iterator();
				while (iter.hasNext()) {
					Row row = iter.next();

					// assumption - row with row number 1 contains the header
					if ( row.getRowNum() == 1) {
						this.buildHeadersInformations(row);
					} else {
						//logger.info( String.valueOf( r.getRowNum() ) );
						this.fillTableRow(row);
					}
				}
				logger.info( this.aircraftsDataTable.structure().print() );
				logger.info( this.aircraftsDataTable.print(10) );
			} 
			wb.close();
		} else {
			logger.info("file <<" + inputExcelFile.getAbsolutePath() + ">> not found or it is not a file");
		}
	}

	public static List<Integer> getAircraftsFoundHeadersColumnIndexes() {
		return aircraftsFoundHeadersColumnIndexes;
	}

	public static List<String> getAircraftsFoundHeaders() {
		return aircraftsFoundHeaders;
	}

	public static List<Integer> getAircraftHeadersColumnIndexes() {
		return getAircraftsFoundHeadersColumnIndexes();
	}

	public static List<String> getAircraftsExpectedHeaders() {
		return aircraftsExpectedHeaders;
	}

	public static String getAircraftsFileName() {
		return aircraftsFileName;
	}

}
