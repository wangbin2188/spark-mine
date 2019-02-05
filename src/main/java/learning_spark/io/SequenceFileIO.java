package learning_spark.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * java中没有saveAsSequenceFile()
 */
public class SequenceFileIO {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Text, IntWritable> input = sc.sequenceFile(args[0], Text.class, IntWritable.class);
        JavaPairRDD<String, Integer> pairRDD = input.mapToPair(new PairFunction<Tuple2<Text, IntWritable>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) throws Exception {
                return new Tuple2<>(record._1.toString(), record._2.get());
            }
        });


        Configuration hadoopConf=new Configuration();
        /**
         * 旧版Hadoop API
         */
        pairRDD.saveAsHadoopFile(args[1],
                Text.class,
                IntWritable.class,
                org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
        /**
         * 新版Hadoop API
         */
        pairRDD.saveAsNewAPIHadoopFile(args[1],
                Text.class,
                IntWritable.class,
                org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class,
                hadoopConf);

    }
}
