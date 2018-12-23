package com.app;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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

    public static List<GenericData.Record> getRecords(Schema schema, String csvPath) {
        List<GenericData.Record> recordList = new ArrayList<>();
        GenericData.Record record;

        try (
                Reader reader = Files.newBufferedReader(Paths.get(csvPath));
//                CSVParser csvFileParser = CSVParser.parse(new File(csvPath), csvFileFormat);
                CSVParser csvParser = CSVParser.parse(reader,CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withIgnoreHeaderCase()
                        .withTrim());

                /*
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withIgnoreHeaderCase()
                        .withTrim());
                */
        ) {

            for (CSVRecord csvRecord : csvParser) {
                record = new GenericData.Record(schema);

                for (int i=0; i<csvRecord.size(); i++) {

                    if (schema.getFields().get(i).schema().getType() == Schema.Type.INT) {
                        record.put(i, Integer.parseInt(csvRecord.get(i)));
                    } else {
                        record.put(i,csvRecord.get(i));
                    }

                }

                recordList.add(record);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return recordList;
    }

    public static void writeToParquet(List<GenericData.Record> recordList, Schema schema, String parquetPath) {

        Path path = new Path(parquetPath);
        ParquetWriter<GenericData.Record> writer = null;
        // Creating ParquetWriter using builder
        try {
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

            for (GenericData.Record record : recordList) {
                writer.write(record);
            }

        }catch(IOException e) {
            e.printStackTrace();
        }finally {
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