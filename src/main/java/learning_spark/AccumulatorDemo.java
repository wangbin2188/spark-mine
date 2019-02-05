package learning_spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

/**
 * 累加器demo
 */
public class AccumulatorDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(args[0]);

        Accumulator<Integer> blankLines = sc.accumulator(0);
        JavaRDD<String> result = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                if (line.equals("")) {
                    blankLines.add(1);
                }
                return Arrays.asList(line.split(" "));
            }
        });
        result.saveAsTextFile(args[1]);
        System.out.println("blank lines:"+blankLines.value());
    }
}
