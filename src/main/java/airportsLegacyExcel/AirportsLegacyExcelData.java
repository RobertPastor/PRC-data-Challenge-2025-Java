package airportsLegacyExcel;

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

import aircrafts.AircraftsData;

public class AirportsLegacyExcelData extends AirportsLegacyExcelDataTable {

	
	private static final Logger logger = Logger.getLogger(AirportsLegacyExcelData.class.getName());

	private static String airportsFileName = "airports.xlsx";

	private String airportsSheetName = "Airports";
	
	private static List<String> airportsExpectedHeaders = Arrays.asList(
			"Id","Name","Airport short name","Country","IATA","ICAO","latitude degrees","longitude degrees","elevation meters");
	
	private static List<Integer> airportsFoundHeadersColumnIndexes = new ArrayList<Integer>();
	private static List<String> airportsFoundHeaders = new ArrayList<String>();

	private static String airportsFolderPath = "C:\\Users\\rober\\eclipse-2025-09\\eclipse-jee-2025-09-R-win32-x86_64\\Data-Challenge-2025\\documents\\airports";

	public AirportsLegacyExcelData() {
		super();
		logger.info("Constructor -> source file = " + AirportsLegacyExcelData.getAirportsFileName());
	}
	
	/**
	 * build the headers while analysing the first row from the EXCEL file
	 * @param r the row
	 */
	private void buildHeadersInformations ( final Row row ) {
		for (Cell cell : row) {
			if (cell != null) {
				logger.info("column index = " + cell.getColumnIndex() + " cell type = " + cell.getType());
				if ( cell.getType().equals(CellType.STRING)) {
					String headerNameFound =  cell.getRawValue();
					logger.info("header name found = " + headerNameFound );
					if ( AirportsLegacyExcelData.getAirportsExpectedHeaders().contains(headerNameFound)) {
						// store header names and column indexes for each header
						AirportsLegacyExcelData.getAirportsFoundHeaders().add(headerNameFound);
						AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().add(cell.getColumnIndex());
					}
				}
			}
		}
		logger.info("expected headers = " + AirportsLegacyExcelData.getAirportsExpectedHeaders().toString());
		logger.info("found headers = " +AirportsLegacyExcelData.getAirportsFoundHeaders().toString());
		logger.info("headers column indexes = " +AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().toString());
	}
	
	/**
	 * read an EXCEL file using fastExcel library
	 * @throws IOException
	 */
	public void readExcelFile() throws IOException {

		this.createEmptyAirportsDataTable();
		logger.info(this.airportsDataTable.structure().print());
		
		logger.info(AirportsLegacyExcelData.getAirportsFileName());
		
		// name of the sheet in the EXCEL file
		String sheetName = airportsSheetName;

		String fileName = AirportsLegacyExcelData.getAirportsFileName();
		Path path = Paths.get( AirportsLegacyExcelData.airportsFolderPath, fileName);

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
					logger.info( "row numer = " + String.valueOf( row.getRowNum() ) );
					// assumption - row with row number 1 contains the header
					if ( row.getRowNum() == 1) {
						this.buildHeadersInformations(row);
					} else {
						logger.info("row number = " + String.valueOf( row.getRowNum() ) );
						this.fillTableRow(row);
					}
				}
				logger.info( this.airportsDataTable.structure().print() );
				logger.info( this.airportsDataTable.print(10) );
			} 
			wb.close();
		} else {
			logger.info("file <<" + inputExcelFile.getAbsolutePath() + ">> not found or it is not a file");
		}
	}
	
	private void fillTableRow(final Row row) {
		
		tech.tablesaw.api.Row tableRow = this.airportsDataTable.appendRow();
		logger.info ("Fill Table row - row count ---> " + this.airportsDataTable.rowCount());

		//"Id","Name","Airport short name","Country","IATA","ICAO","latitude degrees","longitude degrees","elevation meters");

		// Id
		int columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(0);
		Cell cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.NUMBER) ) {
				java.math.BigDecimal bigDecimal = (java.math.BigDecimal)cell.getValue();
				long longValue = bigDecimal.longValueExact();
				int intValue = Math.toIntExact(longValue);
				logger.info("---> airport id = " + String.valueOf(intValue) );
				tableRow.setInt("Id", intValue);
			}
		}

		//Name
		columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(1);
		cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.STRING) ) {
				logger.info("---> airport name = " + cell.getRawValue() );
				tableRow.setString("Name", cell.getRawValue() );
			}
		}
		
		//Airport short name
		columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(2);
		cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.STRING) ) {
				tableRow.setString("Airport short name", cell.getRawValue() );
			}
		}

		//Country
		columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(3);
		cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.STRING) ) {
				tableRow.setString("Country", cell.getRawValue() );
			}
		}
		
		//IATA
		columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(4);
		cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.STRING) ) {
				tableRow.setString("IATA", cell.getRawValue() );
			}
		}
		
		//ICAO
		columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(5);
		cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.STRING) ) {
				tableRow.setString("ICAO", cell.getRawValue() );
			}
		}
		
		//latitude degrees
		columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(6);
		cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.NUMBER) ) {
				tableRow.setDouble("airport latitude degrees", 
						(double) utils.Utils.getDoubleFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
			}
		}

		//longitude degrees
		columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(7);
		cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.NUMBER) ) {
				tableRow.setDouble("airport longitude degrees", 
						(double) utils.Utils.getDoubleFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
			}
		}

		//elevation meters
		columnIndex = AirportsLegacyExcelData.getAirportsFoundHeadersColumnIndexes().get(8);
		cell = row.getCell(columnIndex);
		if (cell != null) {
			if ( cell.getType().equals(CellType.NUMBER) ) {
				tableRow.setFloat("airport elevation meters", 
						(float) utils.Utils.getFloatFromBigDecimal ( (java.math.BigDecimal) cell.getValue() ) );
			}
		}
	}

	public static List<String> getAirportsExpectedHeaders() {
		return airportsExpectedHeaders;
	}

	public static List<Integer> getAirportsFoundHeadersColumnIndexes() {
		return airportsFoundHeadersColumnIndexes;
	}

	public static List<String> getAirportsFoundHeaders() {
		return airportsFoundHeaders;
	}

	public static String getAirportsFileName() {
		return airportsFileName;
	}

	public void extendwithName() {
		// TODO Auto-generated method stub
		
	}
	
}
