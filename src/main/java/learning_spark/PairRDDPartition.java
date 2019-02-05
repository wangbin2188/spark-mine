package learning_spark;

import com.google.common.base.Optional;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *能从分区操作中获益的操作有：
 * cogroup()
 * groupWith()
 * join()
 * leftOuterJoin()
 * rightOuterJoin()
 * groupByKey()
 * reduceByKey()
 * combineByKey()
 * lookup()
 * 对于cogroup，join这样的二元操作，预先进行数据分区会导致其中至少一个RDD（使用分区器的RDD）不发生数据混洗，
 * 如果两个RDD使用同样的分区方式，并且它们还缓存在同样的机器上，那么跨节点的数据混洗就不会发生。
 *
 * 影响分区的操作：
 * map()可能改变key,因此有可能改变分区
 * 因此spark提供了mapValues()和flatMapValues()作为替代方法，它们可以保证每个二元组的key保持不变，
 * 为了最大化分区相关优化的潜在作用，在无需改变key是尽量使用mapValues()和flatMapValues()
 *
 * 所有会为结果RDD设好分区的操作：
 * cogroup()
 * groupWith()
 * join()
 * leftOuterJoin()
 * rightOuterJoin()
 * groupByKey()
 * reduceByKey()
 * combineByKey()
 * partitionBy()
 * sort()
 * mapValues()如果父RDD有分区方式的话
 * 对于二元操作，输出数据的分区方式取决于父RDD的分区方法，默认会采用哈希分区；
 * 分区的数量和操作并行度一样，不过如果其中有一个父RDD已经设置过分区，那么结果就会采用那种分区方式；
 * 如果两个父RDD都设置过分区方式，结果RDD会采用第一个父RDD的分区方式。
 */
public class PairRDDPartition {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("learning_spark.PairRDDPartition");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 2);
            }
        });


        /**
         * 对pairRDD重新分区
         * coalesce()是repartition的优化版
         * 重新分区的数据需要进行持久化，否则每次用到这个数据都需要重新分区
         */
        JavaPairRDD<String, Integer> result3 = pairRDD.repartition(20);
        JavaPairRDD<String, Integer> result4 = pairRDD.coalesce(20, false);
        JavaPairRDD<String, Integer> result5 = pairRDD.partitionBy(new HashPartitioner(15));
        JavaPairRDD<String, Integer> persist = result5.persist(StorageLevel.MEMORY_AND_DISK());

        /**
         * partitionBy，传人自定义的partitioner
         **/
        pairRDD.partitionBy(new DomainNamePartitioner());
        pairRDD.groupByKey(new DomainNamePartitioner());
        pairRDD.join(pairRDD, new DomainNamePartitioner());

        /**
         * 查看当前rdd的分区数
         * 获取RDD的分区方式
         */

        int size = pairRDD.partitions().size();
        Optional<Partitioner> partitioner = pairRDD.partitioner();
        partitioner.isPresent();//判断是否有值
        Partitioner partitioner1 = partitioner.get();
        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> result = pairRDD.groupWith(pairRDD);
    }
}
