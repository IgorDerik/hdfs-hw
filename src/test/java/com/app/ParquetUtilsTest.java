package com.app;

import org.apache.avro.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;

import java.io.File;

import static org.junit.Assert.*;

public class ParquetUtilsTest {

    @Test
    public void writeToParquet() {

        Schema schema = ParquetUtils.parseSchema("src/test/resources/sample.avsc");
        File parquetFile = new File("src/test/resources/fileToBeCreated.parquet");
        assertFalse(parquetFile.exists());
        ParquetUtils.writeToParquet(schema,"src/test/resources/sample.csv",parquetFile.getPath());
        assertTrue(parquetFile.exists());

        //deleting redundant files
        File crc = new File("src/test/resources/.fileToBeCreated.parquet.crc");
        parquetFile.delete();
        crc.delete();
    }

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

    @Test
    public void readParquetFile() {

        ParquetUtils.readParquetFile("src/test/resources/fileForRead.parquet",5);

        assertEquals(  "{\"id\": 0, \"hotel_cluster\": \"a\"}\r\n" +
                "{\"id\": 1, \"hotel_cluster\": \"b\"}\r\n" +
                "{\"id\": 2, \"hotel_cluster\": \"c\"}\r\n" +
                "{\"id\": 3, \"hotel_cluster\": \"d\"}\r\n" +
                "{\"id\": 4, \"hotel_cluster\": \"e\"}\r\n", systemOutRule.getLog());
    }

}