package com.app;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class Main {

    public static void main(String[] args) {
/*
        ParquetUtils.testCSVReader("src/main/resources/test.csv",100);
        Schema schema = ParquetUtils.parseSchema("src/main/resources/t50.avsc");
        List<GenericData.Record> recordList = ParquetUtils.getRecords(schema,"src/main/resources/t50.csv");
        ParquetUtils.writeToParquet(recordList,schema,"src/main/resources/t50.parquet");
        ParquetUtils.readParquetFile("src/main/resources/t50.parquet",100);
*/

        Schema schema = ParquetUtils.parseSchema(args[0]);

        ParquetUtils.writeToParquet(schema,args[1],args[2]);

        ParquetUtils.readParquetFile(args[2],Integer.parseInt(args[3]));

    }

}
