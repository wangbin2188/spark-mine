package learning_spark;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;


public class PairRDDCreateTransform {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        /**
         * 文件名作为key,文件内容作为value
         */
        JavaPairRDD<String, String> pairRDD1 = sc.wholeTextFiles(args[0]);
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], 1);
            }
        });

        /**
         * pairRDD的转化操作
         * reduceByKey()
         * foldByKey()
         * groupByKey()
         * mapValues()只访问value部分
         * combineByKey()
         * keys()
         * values()
         * sortByKey()
         * subtractByKey()
         * join()
         * rightOuterJoin()
         * leftOuterJoin()
         * cogroup()
         */
        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });

        JavaPairRDD<String, Integer> result1 = pairRDD.foldByKey(1, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x * y;
            }
        });
        pairRDD.partitionBy(new DomainNamePartitioner());
        JavaPairRDD<String, Iterable<Integer>> result2 = pairRDD.groupByKey(new DomainNamePartitioner());
        pairRDD.combineByKey(new CreateCombiner(),new MergeValue(),new MergeCombiners());
        JavaPairRDD<String, Integer> result3 = pairRDD.mapValues(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer x) throws Exception {
                return x * 2;
            }
        });

        JavaPairRDD<String, Integer> reslut4 = pairRDD.flatMapValues(new Function<Integer, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(Integer x) throws Exception {
                ArrayList<Integer> list = new ArrayList<>();
                for (int i = 1; i <= x; i++) {
                    list.add(x);
                }
                return list;
            }
        });


        JavaRDD<String> keys = pairRDD.keys();
        JavaRDD<Integer> values = pairRDD.values();
        JavaPairRDD<String, Integer> result5 = pairRDD.sortByKey();
        JavaPairRDD<String, Integer> result6 = pairRDD.subtractByKey(pairRDD);
        JavaPairRDD<String, Tuple2<Integer, Integer>> result7 = pairRDD.join(pairRDD,new DomainNamePartitioner(20));
        JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> result8 = pairRDD.rightOuterJoin(pairRDD);
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> result9 = pairRDD.leftOuterJoin(pairRDD);
        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> result10 = pairRDD.cogroup(pairRDD);


    }

    private static class CreateCombiner implements Function<Integer, Long> {
        @Override
        public Long call(Integer integer) throws Exception {
            return Long.valueOf(integer);
        }
    }

    private static class MergeValue implements Function2<Long, Integer, Long> {
        @Override
        public Long call(Long x, Integer y) throws Exception {
            return x+y;
        }
    }

    private static class MergeCombiners implements Function2<Long, Long, Long> {
        @Override
        public Long call(Long x, Long y) throws Exception {
            return x+y;
        }
    }


}
