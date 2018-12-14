import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
//        System.setProperty("hadoop.home.dir","C:\\hadoop" );

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Cup Matches");
//        conf.set("fs.defaultFS","hdfs://localhost:8020");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> stringRDD = context.textFile("file:///root/matches.csv");
        System.out.println(stringRDD.count());

//        Configuration conf = new Configuration();
//        URI uri = URI.create("hdfs://localhost:8020/user/admin/EHR/train.csv");
//        conf.set("fs.defaultFS","hdfs://localhost:8020");
//        conf.set("fs.defaultFS","swebhdfs://one.hdp:50470");
//        FileSystem fs = FileSystem.get(conf);
//        FSDataInputStream inputStream = fs.open(new Path("/user/admin/matches.csv"));
//        String out = IOUtils.toString(inputStream, "UTF-8");
//        System.out.println(out);
//        inputStream.close();
//        fs.close();
//        FileStatus[] fsStatus = fs.listStatus(new Path("/user/admin/"));
//        for (FileStatus fsStatus1 : fsStatus) { System.out.println(fsStatus1.getPath().toString()); }
//      hdfs://localhost:8020/user/admin/EHR/sample_submission.csv
//        FileSystem csvFile = FileSystem.get(uri, conf);
//        System.out.println(csvFile.hashCode());

//        SparkSession spark = SparkSession.builder().appName("My App").config("spark.master", "local").getOrCreate();
//        SparkSession spark = SparkSession.builder().appName("My App").config("fs.defaultFS","hdfs://localhost:8020").getOrCreate();
//        Dataset<Row> df = spark.read().csv("src/main/resources/matches.csv");
//        Dataset<Row> df = spark.read().csv("hdfs://localhost:8020/user/admin/matches.csv");
//        Dataset<Row> df = spark.read().csv("user/admin/EHR/sample_submission.csv");
//        Dataset<Row> df = spark.read().csv()
//        df.show();
//        df.write().parquet("src/main/resources/matches");


//        spark.close();
    }
}
