package learning_spark.io;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class DataBaseIO {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame user3 = sqlContext.jdbc("jdbc:mysql://localhost/test?user=holden", "user3");
    }
}

