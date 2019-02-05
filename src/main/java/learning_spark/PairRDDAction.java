package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class PairRDDAction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        /**
         * pairRDD上的行动操作：
         * countByKey()
         * collectAsMap()
         * lookup()
         */
        Map<String, Object> result = pairRDD.countByKey();
        Map<String, Integer> result1 = pairRDD.collectAsMap();
        List<Integer> result2 = pairRDD.lookup("");
        Map<Tuple2<String, Integer>, Long> result3 = pairRDD.countByValue();

    }
}
