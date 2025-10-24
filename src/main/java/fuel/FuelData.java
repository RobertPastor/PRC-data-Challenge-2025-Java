package fuel;


import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.jerolba.carpet.CarpetReader;

import dataChallengeEnums.DataChallengeEnums.train_rank;
import folderDiscovery.FolderDiscovery;
import fuel.FuelDataSchema.FuelDataRecord;
import tech.tablesaw.api.Table; 


public class FuelData extends FuelDataTable {

	private train_rank train_rank_value;

	public train_rank getTrain_rank_value() {
		return train_rank_value;
	}

	public FuelData( train_rank value ) {
		this.train_rank_value = value;
	}

	public Table getFuelDataTable() {
		return fuelDataTable;
	}

	public void readParquet() throws IOException {
		this.createEmptyFuelDataTable();

		try {
			FolderDiscovery folderDiscovery = new FolderDiscovery();

			File file = folderDiscovery.getFuelFileFromFileName(this.getTrain_rank_value());
			if ( (file != null) && file.exists() && file.isFile()) {

				System.out.println("file = " + file.getAbsolutePath() + " found !!!");
				var reader = new CarpetReader<>(file, FuelDataRecord.class);
				Iterator<FuelDataRecord> iterator = ((CarpetReader<FuelDataRecord>) reader).iterator();
				int count = 0;

				while (iterator.hasNext()) {
					FuelDataRecord r = iterator.next();
					//System.out.println(r);

					this.appendRowToFuelDataTable(r);

					if (count > 10) {
						//break;
					}
					count = count + 1;
				}
				System.out.println(this.fuelDataTable.print(10));
				
			} else {
				System.out.println("Error -> file not found -> in repo -> " + this.getTrain_rank_value() );
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		System.out.println("Parquet file <<" + this.getTrain_rank_value() + ">> Fuel read successfully!");
	}
}
