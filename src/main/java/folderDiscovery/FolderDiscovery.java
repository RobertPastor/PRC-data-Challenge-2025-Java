package folderDiscovery;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

import dataChallengeEnums.DataChallengeEnums.train_rank_final;

public class FolderDiscovery {
	
	private static final Logger logger = Logger.getLogger(FolderDiscovery.class.getName());

	String className = "";
	
	//private static String flightTrainFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-train-data-interpolated/";
	private static String flightTrainFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-train-data/";
	//private static String flightRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-rank-data-interpolated/";
	private static String flightRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-rank-data/";
	//private static String flightFinalFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-final-data-interpolated/";
	private static String flightFinalFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-final-data/";
	
	public static String getFuelFinalFolderStr() {
		return fuelFinalFolderStr;
	}

	public static void setFuelFinalFolderStr(String fuelFinalFolderStr) {
		FolderDiscovery.fuelFinalFolderStr = fuelFinalFolderStr;
	}

	public static String getTrainRankFinalOutputfolderStr() {
		return trainRankFinalOutputfolderStr;
	}

	public static void setTrainRankFinalOutputfolderStr(String trainRankFinalOutputfolderStr) {
		FolderDiscovery.trainRankFinalOutputfolderStr = trainRankFinalOutputfolderStr;
	}

	public File getFlightFinalFolder() {
		return flightFinalFolder;
	}

	public void setFlightFinalFolder(File flightFinalFolder) {
		this.flightFinalFolder = flightFinalFolder;
	}

	public File[] getFlightFinalFiles() {
		return flightFinalFiles;
	}

	public void setFlightFinalFiles(File[] flightFinalFiles) {
		this.flightFinalFiles = flightFinalFiles;
	}

	public static void setFlightTrainFolderStr(String flightTrainFolderStr) {
		FolderDiscovery.flightTrainFolderStr = flightTrainFolderStr;
	}

	public static void setFlightRankFolderStr(String flightRankFolderStr) {
		FolderDiscovery.flightRankFolderStr = flightRankFolderStr;
	}

	public static void setFlightFinalFolderStr(String flightFinalFolderStr) {
		FolderDiscovery.flightFinalFolderStr = flightFinalFolderStr;
	}

	public static void setFuelRankFolderStr(String fuelRankFolderStr) {
		FolderDiscovery.fuelRankFolderStr = fuelRankFolderStr;
	}

	public static void setFlightListTrainRankFolderStr(String flightListTrainRankFolderStr) {
		FolderDiscovery.flightListTrainRankFolderStr = flightListTrainRankFolderStr;
	}

	public static void setLegacyAirportsFolderStr(String legacyAirportsFolderStr) {
		FolderDiscovery.legacyAirportsFolderStr = legacyAirportsFolderStr;
	}

	public static void setAirportsLegacyFolderStr(String airportsLegacyFolderStr) {
		FolderDiscovery.airportsLegacyFolderStr = airportsLegacyFolderStr;
	}

	public static void setAircraftsFolderPath(String aircraftsFolderPath) {
		FolderDiscovery.aircraftsFolderPath = aircraftsFolderPath;
	}

	public void setFlightTrainFolder(File flightTrainFolder) {
		this.flightTrainFolder = flightTrainFolder;
	}

	public void setFlightRankFolder(File flightRankFolder) {
		this.flightRankFolder = flightRankFolder;
	}

	private static String fuelRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/Fuel/";
	private static String fuelTrainFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/Fuel/";
	private static String fuelFinalFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/Fuel/";
	
	private static String flightListTrainRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/FlightList";
	
	// 4th November 2025 - airports provided by the PRC team with missing elevation on one airport used in the rank flight list
	private static String airportsFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/Environment/AirportsDataChallenge";
	private static String legacyAirportsFolderStr = "C:/Users/rober/eclipse-2025-09/eclipse-jee-2025-09-R-win32-x86_64/Data-Challenge-2025/documents/airports";

	// legacy airports EXCEL file replacing apt.parquet because airport ZGOW "Jieyang Chaoshan International Airport" has missing elevation_ft value
	private static String airportsLegacyFolderStr = "C:/Users/rober/eclipse-2025-09/eclipse-jee-2025-09-R-win32-x86_64/Data-Challenge-2025/documents/airports";
	
	private static String trainRankFinalOutputfolderStr = "C:/Users/rober/eclipse-2025-09/eclipse-jee-2025-09-R-win32-x86_64/Data-Challenge-2025/documents";

	private static String aircraftsFolderPath = "C:/Users/rober/eclipse-2025-09/eclipse-jee-2025-09-R-win32-x86_64/Data-Challenge-2025/documents/aircrafts";

	
	public static String getAircraftsFolderPath() {
		return aircraftsFolderPath;
	}

	public static String getFuelTrainFolderStr() {
		return fuelTrainFolderStr;
	}

	public static String getFuelRankFolderStr() {
		return fuelRankFolderStr;
	}

	public static String getFlightListTrainRankFolderStr() {
		return flightListTrainRankFolderStr;
	}

	public static String getTrainRankOutputfolderStr() {
		return trainRankFinalOutputfolderStr;
	}

	public static void setFuelTrainFolderStr(String fuelTrainFolderStr) {
		FolderDiscovery.fuelTrainFolderStr = fuelTrainFolderStr;
	}

	public static void setAirportsFolderStr(String airportsFolderStr) {
		FolderDiscovery.airportsFolderStr = airportsFolderStr;
	}

	public static String getAirportsFolderStr() {
		return airportsFolderStr;
	}

	private File flightTrainFolder = null;
	private File flightRankFolder = null;
	private File flightFinalFolder = null;
	
	private File[] flightTrainFiles = null;
	private File[] flightRankFiles = null;
	private File[] flightFinalFiles = null;
	
	public static String getFlightTrainFolderStr() {
		return flightTrainFolderStr;
	}
	
	public static String getFlightRankFolderStr() {
		return flightRankFolderStr;
	}

	public File getFlightTrainFolder() {
		return flightTrainFolder;
	}


	public File getFlightRankFolder() {
		return flightRankFolder;
	}


	public File[] getFlightRankFiles() {
		return flightRankFiles;
	}

	public FolderDiscovery() {
		this.className = FolderDiscovery.class.getSimpleName();
	}
	
	public File getFlightFileFromFileName ( train_rank_final train_rank_value , String fileName ) {
		
		logger.info(this.className + " --- " + fileName );
		if (!fileName.endsWith("parquet")) {
			fileName = fileName + ".parquet";
		}
		try {
			Path path = null;
			if ( train_rank_value == train_rank_final.train) {
				path = Paths.get(FolderDiscovery.flightTrainFolderStr , fileName);
			} else {
				if ( train_rank_value == train_rank_final.rank ) {
					path = Paths.get(FolderDiscovery.flightRankFolderStr , fileName);
				}else {
					path = Paths.get(FolderDiscovery.flightFinalFolderStr , fileName);
				}
			}
			logger.info(path.toString());
			File file = path.toFile();
			if (!file.exists() || !file.isFile()) {
				return null;
			} else {
				return file;
			}
		} catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
		return null;
	}
	
	public File getFuelFileFromFileName( final train_rank_final train_rank_value ) {
		
		try {
			Path path =  null;
			if ( train_rank_value == train_rank_final.rank ) {
				path = Paths.get(FolderDiscovery.fuelRankFolderStr , "fuel_rank_submission.parquet" );
			} else {
				if ( train_rank_value == train_rank_final.train ) {
					path = Paths.get(FolderDiscovery.fuelTrainFolderStr , "fuel_train.parquet");
				} else {
					path = Paths.get(FolderDiscovery.fuelFinalFolderStr , "fuel_final_submission.parquet");
				}
			}
			File file = path.toFile();
			if (!file.exists() || !file.isFile()) {
				return null;
			} else {
				logger.info(file.getAbsolutePath());
				return file;
			}
		} catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
		return null;
	}
	
	public void discover() {
		this.flightTrainFolder = new File(flightTrainFolderStr);
		this.flightRankFolder = new File(flightRankFolderStr);
		this.flightFinalFolder = new File(flightFinalFolderStr);
		
		if ( this.flightTrainFolder.exists() && this.flightTrainFolder.isDirectory()) {
			this.setFlightTrainFiles(this.flightTrainFolder.listFiles());
		}
		if ( this.flightRankFolder.exists() && this.flightRankFolder.isDirectory()) {
			this.setFlightRankFiles(this.flightRankFolder.listFiles());
		}
		if ( this.flightFinalFolder.exists() && this.flightFinalFolder.isDirectory()) {
			this.setFlightFinalFiles(this.flightFinalFolder.listFiles());
		}
	}
	
	public int getFlightFolderNbFiles( train_rank_final train_rank_value ) {
		if ( train_rank_value == train_rank_final.rank ) {
			return this.getRankFolderNbFiles();
		} else {
			if ( train_rank_value == train_rank_final.train ) {
				return this.getTrainFolderNbFiles();
			} else {
				return this.getFinalFolderNbFiles();
			}
		}
	}
	
	public int getTrainFolderNbFiles() {
		return this.flightTrainFiles.length;
	}
	
	public int getRankFolderNbFiles() {
		return this.flightRankFiles.length;
	}

	public int getFinalFolderNbFiles() {
		return this.flightFinalFiles.length;
	}
	
	public File[] getFlightTrainFiles() {
		return flightTrainFiles;
	}

	public void setFlightTrainFiles(File[] flightTrainFiles) {
		this.flightTrainFiles = flightTrainFiles;
	}
	
	public void setFlightRankFiles(File[] flightRankFiles) {
		this.flightRankFiles = flightRankFiles;
	}

	public File getFlightListFileFromFileName(train_rank_final train_rank_value) {
		String fileName = "flightlist_train.parquet";
		
		if ( train_rank_value == train_rank_final.train) {
			fileName = "flightlist_train.parquet";
		}
		
		if ( train_rank_value == train_rank_final.rank) {
			fileName = "flightlist_rank.parquet";
		}
		
		if ( train_rank_value == train_rank_final.final_submission) {
			fileName = "flightlist_final.parquet";
		}
		
		Path path = Paths.get(FolderDiscovery.flightListTrainRankFolderStr , fileName);
		File file = path.toFile();
		if ( file.exists() && file.isFile()) {
			logger.info(file.getAbsolutePath());
			return file;
		}
		return null;
	}

	public File getAirportsFileFromFileName() {
		String fileName = "apt.parquet";
		Path path = Paths.get(FolderDiscovery.getAirportsFolderStr() , fileName);
		File file = path.toFile();
		if ( file.exists() && file.isFile()) {
			logger.info(file.getAbsolutePath());
			return file;
		}
		return null;
	}

	public String getFlightPathAsString(train_rank_final train_rank_value, String fileName) {
		String folder = "";
		if ( train_rank_value == train_rank_final.rank) {
			folder = flightRankFolderStr;
		} else {
			if ( train_rank_value == train_rank_final.rank) {
				folder = flightTrainFolderStr;
			} else {
				folder = flightFinalFolderStr;
			}
		}
		Path path = Paths.get(folder , fileName);
		File file = path.toFile();
		if ( file.exists() && file.isFile()) {
			logger.info(file.getAbsolutePath());
			return file.getAbsolutePath();
		}
		return null;
	}

	public static String getAirportsLegacyFolderStr() {
		return airportsLegacyFolderStr;
	}

	public static String getLegacyAirportsFolderStr() {
		return legacyAirportsFolderStr;
	}
	
	public static String getFlightFinalFolderStr() {
		return flightFinalFolderStr;
	}

}
