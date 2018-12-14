package com.sparkapp;
import org.apache.spark.sql.*;

public class Main {

    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir","C:\\hadoop" );

        SparkSession spark = SparkSession.builder().appName("My App").config("spark.master", "local").getOrCreate();
        Dataset<Row> df = spark.read().csv(args[0]);
        df.write().parquet(args[1]);
    }

}
