package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class RDDCreate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        /**
         * 创建RDD两种方式：
         * 1.从一个集合创建RDD
         * 2.从一个外部存储读取数据创建RDD
         */
        JavaRDD<Integer> lines = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        JavaRDD<String> lines2 = sc.textFile(args[0]);

        lines2.saveAsTextFile(args[1]);



    }




}
