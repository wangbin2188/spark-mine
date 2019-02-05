package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(args[0]);
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> wordCounts = counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });

        wordCounts.saveAsTextFile(args[1]);
    }
}
