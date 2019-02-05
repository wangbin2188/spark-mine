package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;

import java.util.List;

/**
 * 数值RDD操作
 */
public class NumericalRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaDoubleRDD doubleRDD = lines.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String s) throws Exception {
                return Double.parseDouble(s);
            }
        });
        StatCounter stats = doubleRDD.stats();
        double stdev = stats.stdev();
        double mean = stats.mean();
        long count = stats.count();
        double max = stats.max();

        JavaDoubleRDD filter = doubleRDD.filter(new Function<Double, Boolean>() {
            @Override
            public Boolean call(Double aDouble) throws Exception {
                return aDouble > 5;
            }
        });

        List<Double> collect = filter.collect();
        System.out.println(collect);
    }
}
