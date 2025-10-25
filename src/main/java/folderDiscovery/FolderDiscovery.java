package folderDiscovery;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import fuel.FuelDataTable;

public class FolderDiscovery {
	
	private static final Logger logger = Logger.getLogger(FolderDiscovery.class.getName());

	
	String className = "";
	
	private static String flightTrainFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-train-data/";
	private static String flightRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-rank-data/";
	
	private static String fuelRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/Fuel/";
	private static String fuelTrainFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/Fuel/";
	
	private static String flightListTrainRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/FlightList";
	private static String airportsFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/Environment/AirportsDataChallenge";
	
	public static String getAirportsFolderStr() {
		return airportsFolderStr;
	}

	private File flightTrainFolder = null;
	private File flightRankFolder = null;
	
	private File[] flightTrainFiles = null;
	private File[] flightRankFiles = null;
	
	public static String getFlightTrainFolderStr() {
		return flightTrainFolderStr;
	}
	
	public static String getFlightRankFolderStr() {
		return flightRankFolderStr;
	}

	public void setFlightTrainFolderStr(String flightTrainFolderStr) {
		FolderDiscovery.flightTrainFolderStr = flightTrainFolderStr;
	}

	public void setFlightRankFolderStr(String flightRankFolderStr) {
		FolderDiscovery.flightRankFolderStr = flightRankFolderStr;
	}

	public File getFlightTrainFolder() {
		return flightTrainFolder;
	}

	public void setFlightTrainFolder(File flightTrainFolder) {
		this.flightTrainFolder = flightTrainFolder;
	}

	public File getFlightRankFolder() {
		return flightRankFolder;
	}

	public void setFlightRankFolder(File flightRankFolder) {
		this.flightRankFolder = flightRankFolder;
	}

	public File[] getFlightRankFiles() {
		return flightRankFiles;
	}

	public FolderDiscovery() {
		this.className = FolderDiscovery.class.getSimpleName();
	}
	
	public File getFlightFileFromFileName ( train_rank train_rank_value , String fileName ) {
		
		//logger.info(this.className + " --- " + fileName );
		if (!fileName.endsWith("parquet")) {
			fileName = fileName + ".parquet";
		}
		try {
			Path path = null;
			if ( train_rank_value == train_rank.rank) {
				path = Paths.get(FolderDiscovery.flightRankFolderStr , fileName);
			} else {
				path = Paths.get(FolderDiscovery.flightTrainFolderStr , fileName);
			}
			
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
	
	public File getFuelFileFromFileName( final train_rank train_rank_value ) {
		
		try {
			Path path =  null;
			if ( train_rank_value == train_rank.rank ) {
				path = Paths.get(FolderDiscovery.fuelRankFolderStr , "fuel_rank_submission.parquet" );
			} else {
				path = Paths.get(FolderDiscovery.fuelTrainFolderStr , "fuel_train.parquet");
			}
			File file = path.toFile();
			if (!file.exists() || !file.isFile()) {
				return null;
			} else {
				System.out.println(file.getAbsolutePath());
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
		
		if ( this.flightTrainFolder.exists() && this.flightTrainFolder.isDirectory()) {
			this.setFlightTrainFiles(this.flightTrainFolder.listFiles());
		}
		if ( this.flightRankFolder.exists() && this.flightRankFolder.isDirectory()) {
			this.setFlightRankFiles(this.flightRankFolder.listFiles());
		}
	}
	
	public int getFlightFolderNbFiles( train_rank train_rank_value ) {
		if ( train_rank_value == train_rank.rank ) {
			return this.getRankFolderNbFiles();
		} else {
			return this.getTrainFolderNbFiles();
		}
	}
	
	public int getTrainFolderNbFiles() {
		return this.flightTrainFiles.length;
	}
	
	public int getRankFolderNbFiles() {
		return this.flightRankFiles.length;
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

	public File getFlightListFileFromFileName(train_rank train_rank_value) {
		String fileName = "flightlist_train.parquet";
		if ( train_rank_value == train_rank.rank) {
			fileName = "flightlist_rank.parquet";
		}
		Path path = Paths.get(FolderDiscovery.flightListTrainRankFolderStr , fileName);
		File file = path.toFile();
		if ( file.exists() && file.isFile()) {
			return file;
		}
		return null;
	}

	public File getAirportsFileFromFileName() {
		String fileName = "apt.parquet";
		Path path = Paths.get(FolderDiscovery.getAirportsFolderStr() , fileName);
		File file = path.toFile();
		if ( file.exists() && file.isFile()) {
			return file;
		}
		return null;
	}

	public String getFlightPath(train_rank train_rank_value, String fileName) {
		String folder = "";
		if ( train_rank_value == train_rank.rank) {
			folder = flightRankFolderStr;
		} else {
			folder = flightTrainFolderStr;
		}
		Path path = Paths.get(folder , fileName);
		File file = path.toFile();
		if ( file.exists() && file.isFile()) {
			return file.getAbsolutePath();
		}
		return null;
	}

}
