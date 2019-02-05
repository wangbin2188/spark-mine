package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class InitSparkContext {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //你的代码
        sc.stop();
    }
}
