package genericCarpetParquetWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.io.FileSystemOutputFile;

import genericCarpetParquetWriter.SampleDataFactory.Org;


public class WriteToParquetUsingCarpetWriter {

	static void generateParquetFileFor() throws IOException{
		
        try {
        	List<Org> organizations = new SampleDataFactory().getOrganizations(40);
	      
        	File file = new File("./organizations.parquet");
	        FileSystemOutputFile outputFile = new FileSystemOutputFile(file);
	        try (CarpetWriter<Org> writer = new CarpetWriter<>(outputFile, Org.class)) {
	        	for (Org org : organizations) {
	                writer.write(org);
	            }
	        }
	        
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
        
        System.out.println("Parquet file written successfully!");
	 }
}
