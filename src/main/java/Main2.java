import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main2 {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\hadoop" );

        SparkSession spark = SparkSession.builder().appName("My App").config("spark.master", "local").getOrCreate();
        Dataset<Row> df = spark.read().csv("src/main/resources/matches.csv");
//        Dataset<Row> df = spark.read().csv("hdfs://localhost:8020/user/admin/EHR/sample_submission.csv");
//        df.show();
        df.write().parquet("src/main/resources/matches-parquet1");
    }

}