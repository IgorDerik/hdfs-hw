package com.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class Utils {

    static void convertCsvToParquet(String csvPath, String parquetPath) {
        SparkSession spark = SparkSession.builder().appName("My App").config("spark.master", "local").getOrCreate();
//        Dataset<Row> df = spark.read().csv(csvPath);
        Dataset<Row> df = spark.read().format("csv").option("header", "true").load(csvPath);
        df.write().parquet(parquetPath);
    }

    static void showFiles(String rootPath, String targetPath) throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", rootPath);
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] fsStatus = fs.listStatus(new Path(targetPath));
        for (FileStatus status : fsStatus) {
            System.out.println(status.getPath().toString());
        }
    }

}
