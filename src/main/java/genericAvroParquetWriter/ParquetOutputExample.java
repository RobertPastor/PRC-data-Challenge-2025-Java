package genericAvroParquetWriter;


import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;

import java.io.IOException;
import java.nio.file.Paths;

public class ParquetOutputExample {
    public static void main(String[] args) throws IOException {
        // Define schema
        MessageType schema = Types.buildMessage()
                .required(Types.primitive(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY, 
                		org.apache.parquet.schema.Type.Repetition.REQUIRED).named("name"))
                .required(Types.primitive(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32, 
                		org.apache.parquet.schema.Type.Repetition.REQUIRED).named("age"))
                .named("Person");

        // Create OutputFile (local file system)
        java.nio.file.Path path = Paths.get("output.parquet");
        OutputFile outputFile = new LocalOutputFile(path);

        // Create ParquetWriter
        try (ParquetWriter<Group> writer =  new ExampleParquetWriter(
                outputFile,
                schema,
                CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetProperties.WriterVersion.PARQUET_1_0)) {

            // Write data
            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
            Group group = groupFactory.newGroup()
                    .append("name", Binary.fromString("Alice"))
                    .append("age", 30);
            writer.write(group);
        }

        System.out.println("Parquet file written successfully!");
    }
}
