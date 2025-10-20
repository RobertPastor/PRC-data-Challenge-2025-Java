package genericAvroParquetWriter;


import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.io.

import org.apache.parquet.io.OutputFile;

public class ParquetWriterExample {
	
	public static void main(String[] args) throws Exception {
		String path = "./myFirst.parquet";
		OutputFile outputFile = new OutputFile(path);

        MessageType schema = new MessageType("example.Schema",
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "name"));

        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        SimpleGroup group = (SimpleGroup) factory.newGroup();
        group.append("name", "John Doe");
        
        ParquetWriter<SimpleGroup> writer = ExampleParquetWriter.write(path,schema);

        writer.write(group);
        writer.close();
    }
}