package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * RDD上的transform操作
 * map()
 * filter()过滤
 * union()并
 * intersection()交
 * subtract()减
 * distinct()去重
 * cartesian()笛卡尔积
 * flatMap()
 * mapToPair()
 */
public class RDDTransform {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("operateRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        /**
         * 分别使用匿名内部类(lambda表达式)和具名类进行参数传递
         */
        JavaRDD<String> errorRDD = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("error");
            }
        });
        JavaRDD<String> map = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s;
            }
        });
        lines.filter(s -> s.contains("error"));

        JavaRDD<String> warnRDD = lines.filter(new WarnFilter("warn"));

        JavaRDD<String> union = errorRDD.union(warnRDD);
        JavaRDD<String> intersection = errorRDD.intersection(warnRDD);
        JavaRDD<String> subtract = errorRDD.subtract(warnRDD);
        JavaPairRDD<String, String> cartesian = errorRDD.cartesian(warnRDD);
        union.distinct();
        union.sample(false, 0.5);

        union.take(10);
        union.count();
    }

    private static class WarnFilter implements Function<String, Boolean> {

        private String warn;

        public WarnFilter(String warn) {
            this.warn = warn;
        }

        @Override
        public Boolean call(String s) throws Exception {
            return s.contains(warn);
        }
    }

}
