package com.app;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetUtils {

    public static Schema parseSchema(String schemaPath) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;

        try {
            schema = parser.parse(new File(schemaPath));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return schema;
    }

    public static void testCSVReader(String csvPath, int maxRows) {

        try (
                Reader reader = Files.newBufferedReader(Paths.get(csvPath));
                CSVReader csvReader = new CSVReader(reader);
//                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        ) {

            int i = maxRows;
            String[] nextRecord;

            while ( ((nextRecord = csvReader.readNext()) != null) && i>0 ) {

                for(String s : nextRecord) {
                    System.out.print(s+" ");
                }
                System.out.println();

                i--;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void writeToParquet(Schema schema, String csvPath, String parquetPath) {

        Path path = new Path(parquetPath);
        ParquetWriter<GenericData.Record> writer = null;
        GenericData.Record record;

        try (
                Reader reader = Files.newBufferedReader(Paths.get(csvPath));
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        ) {
            String[] nextRecord;

            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();

            while ((nextRecord = csvReader.readNext()) != null) {
                record = new GenericData.Record(schema);

                for (int i=0; i<nextRecord.length; i++) {

                    Schema.Type type = schema.getFields().get(i).schema().getType();

                    if (type == Schema.Type.INT) {
                        int putRecord = nextRecord[i].isEmpty() ? 0 : Integer.parseInt(nextRecord[i]);
                        record.put(i, putRecord);
                    } else if (type == Schema.Type.DOUBLE) {
                        double putRecord = nextRecord[i].isEmpty() ? 0.0d : Double.parseDouble(nextRecord[i]);
                        record.put(i, putRecord);
                    } else {
                        record.put(i,nextRecord[i]);
                    }
                }
                writer.write(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void readParquetFile(String parquetFilePath, int maxRows) {
        ParquetReader<GenericData.Record> reader = null;

        Path path = new Path(parquetFilePath);
        try {
            reader = AvroParquetReader
                    .<GenericData.Record>builder(path)
                    .withConf(new Configuration())
                    .build();
            GenericData.Record record;

            int i = maxRows;
            while ( ((record = reader.read()) != null) && i>0 ) {
                System.out.println(record);
                i--;
            }
        }catch(IOException e) {
            e.printStackTrace();
        }finally {
            if(reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

}