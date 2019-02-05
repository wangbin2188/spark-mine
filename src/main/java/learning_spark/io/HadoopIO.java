package learning_spark.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HadoopIO {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hadoopConf=new Configuration();

        /**
         * 旧版Hadoop API
         */
        JavaPairRDD<Text, Text> pairRDD =
                sc.hadoopFile(args[0],
                        org.apache.hadoop.mapred.InputFormat.class,
                        Text.class,
                        Text.class);
        /**
         * 新版Hadoop API
         */
        JavaPairRDD<Text, Text> pairRDD1 =
                sc.newAPIHadoopFile(args[0],
                        org.apache.hadoop.mapreduce.InputFormat.class,
                        Text.class,
                        Text.class,
                        hadoopConf);


    }
}
