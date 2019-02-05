package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 基于分区进行操作可以让我们避免为每个数据元素进行重复的配置工作，诸如打开数据库连接或创建随机数生成器等
 * 都是我们应该尽量避免为每个元素都配置一次的工作
 * spark提供了基于分区的map(注：mapPartitions())和foreach（注：foreachPartitions()）
 * 让代码只对每个分区运行一次
 * mapPartitionsToPair()
 * mapPartitions()
 * foreachPartition()
 * mapPartitionsWithIndex()
 */
public class PartitionBased {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(args[0]);
        JavaPairRDD<String, String> result = rdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Iterator<String> input) throws Exception {
                ArrayList<Tuple2<String, String>> tuple2s = new ArrayList<>();
                while (input.hasNext()) {
                    String line = input.next();
                    String[] split = line.split(",");
                    tuple2s.add(new Tuple2<>(split[0],split[1]));
                }
                return tuple2s;
            }
        });


        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> si) throws Exception {
                while (si.hasNext()) {
                    System.out.println(si.next());
                }
            }
        });

        result.saveAsTextFile(args[1]);


    }
}
