package fuelData;


import java.io.File;
import dataChallengeEnums.DataChallengeEnums.train_rank;
import java.io.IOException;
import java.util.Iterator;

import com.jerolba.carpet.CarpetReader;

import flightData.FlightDataSchema.FlightDataRecord;
import folderDiscovery.FolderDiscovery;

import tech.tablesaw.api.*; 


public class FuelData  {

	private Table fuelDataTable = null;
	
	private train_rank train_rank_value;
	
	public train_rank getTrain_rank_value() {
		return train_rank_value;
	}
	
	FuelData( train_rank value ) {
		this.train_rank_value = value;
	}
}
