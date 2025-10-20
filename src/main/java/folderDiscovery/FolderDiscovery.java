package folderDiscovery;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import dataChallengeEnums.DataChallengeEnums.train_rank;


public class FolderDiscovery {
	
	String className = "";
	
	private static String flightTrainFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-train-data/";
	private static String flightRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/Data-Download-OpenSkyNetwork/competition-rank-data/";
	
	private static String fuelRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/Fuel/";
	
	private static String flightListTrainRankFolderStr = "C:/Users/rober/git/PRCdataChallenge2025/trajectory/FlightList";
	
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
		
		System.out.println(this.className + " --- " + fileName );
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
	
	public File getRankFuelFileFromFileName( String fileName ) {
		
		System.out.println(this.className + " --- " + fileName );
		if (!fileName.endsWith("parquet")) {
			fileName = fileName + ".parquet";
		}
		try {
			Path path = Paths.get(FolderDiscovery.fuelRankFolderStr , fileName);
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
		// TODO Auto-generated method stub
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

}
