package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * spark-submit
 * --master yarn
 * --class com.Test
 * --files filename.properties jarName.jar
 *
 */
public class BroadcastDemo {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(args[0]);
        Properties props = loadCallSignTable();
        Broadcast<Properties> sign = sc.broadcast(props);
        rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String key) throws Exception {
                Properties props = sign.getValue();
                String value = props.getProperty(key);
                return new Tuple2<>(value,1);
            }
        });
    }





    private static Properties loadCallSignTable() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream("filename.properties"));
        return props;
    }
}
