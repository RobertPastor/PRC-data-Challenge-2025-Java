package genericAvroParquetWriter;


import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType; 


public class GenericAvroParquetWriter {

	public static final int DEFAULT_BLOCK_SIZE = 134217728;
	  
	  // Field descriptor #79 I
	  public static final int DEFAULT_PAGE_SIZE = 1048576;
	
	 public static void main(String[] args) throws Exception {

        final String schemaLocation = "./avro_format.json";
        final Schema avroSchema = new Schema.Parser().parse(new File(schemaLocation));
        final MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
        final WriteSupport<Pojo> writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
        final String parquetFile = "./data.parquet";
        
        //final Path path = Paths.get(parquetFile);
        
        /**
         * ParquetWriter<GenericRecord> parquetWriter = new ParquetWriter<GenericRecord>(path, writeSupport, 
         */
        /*		CompressionCodecName.SNAPPY, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
         * final GenericRecord record = new GenericData.Record(avroSchema);
         *       record.put("id", 1);
         *       record.put("age", 10);
         *       record.put("name", "ABC");
         *        record.put("place", "BCD");
         *       parquetWriter.write(record);
         */
        
        //parquetWriter.close();
    }
}
