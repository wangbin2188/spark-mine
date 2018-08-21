import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;

/**
 * Created by wangbin10 on 2018/6/4.
 * select  dvid,type,count(1) from bglogs.app_base_log
 * where day='${day}'
 * and type  in ('headline_tab_click','community_tab_click','car_tab_click','service_tab_click','my_tab_click')
 * group by dvid,type;
 */
public class AppBaseLogToHive {

    public static final String SOURCE = "hdfs://";
    public static final String TARGET = "hdfs://";

    public static void main(final String[] args) throws IOException, InterruptedException {
        /**LZO解压缩的相关配置参数*/
        Configuration configuration = new Configuration();
        configuration.set("mapred.output.compress", "true");
        configuration.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
        configuration.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,com.hadoop.compression.lzo.LzopCodec");
        configuration.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "AppBaseLogToHive", conf);

        /**程序运行之前，先删除需要写入的hdfs文件夹*/
        deleteHdfs(TARGET + args[0], configuration);
        /**
         * LZO压缩文件需要使用特殊的方法读取，
         * Text转String不能直接强转，可能会乱码
         * */
        JavaPairRDD<LongWritable, Text> pairRDD = sc.newAPIHadoopFile(SOURCE + args[0], LzoTextInputFormat.class, LongWritable.class, Text.class, configuration);

        JavaRDD<String> lines = pairRDD.map(new Function<Tuple2<LongWritable, Text>, String>() {

            public String call(Tuple2<LongWritable, Text> text) throws Exception {
                return new String(text._2.getBytes(), 0, text._2.getLength(), "GBK");
            }
        });
        JavaRDD<String[]> records = lines.map(new Function<String, String[]>() {

            public String[] call(String s) throws Exception {
                return s.split("\t");
            }
        });
        JavaRDD<String[]> filterRecords = records.filter(new Function<String[], Boolean>() {

            public Boolean call(String[] strings) throws Exception {
                return strings[2].endsWith("tab_click");
            }
        });
        JavaPairRDD<String[], Integer> column = filterRecords.mapToPair(new PairFunction<String[], String[], Integer>() {


            public Tuple2<String[], Integer> call(String[] strings) throws Exception {
                return new Tuple2<String[], Integer>(new String[]{strings[7], strings[2]}, 1);
            }
        });

        JavaPairRDD<String[], Integer> groups = column.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer integer1, Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        });

        JavaRDD<String> result = groups.map(new Function<Tuple2<String[], Integer>, String>() {

            public String call(Tuple2<String[], Integer> tuple2) throws Exception {
                StringBuilder sb = new StringBuilder();
                sb.append(tuple2._1[0]);
                sb.append("\001");
                sb.append(tuple2._1[1]);
                sb.append("\001");
                sb.append(tuple2._2.toString());
                sb.append("\001");
                sb.append(args[0].toString());
                return sb.toString();
            }
        });

        result.saveAsTextFile(TARGET + args[0]);
    }

    /**
     * 首先判断目标文件夹是否存在，如果存在则删除
     */
    public static void deleteHdfs(String uri, Configuration conf) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(uri), conf, "shtermuser");
        Path dfs = new Path(uri);
        boolean isExists = fs.exists(dfs);
        if (isExists) {
            fs.delete(dfs, true);
        }
    }
}
