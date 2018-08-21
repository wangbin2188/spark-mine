import com.hadoop.compression.lzo.LzopCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by wangbin10 on 2018/7/16.
 * updated by wangbin10 on 2018/7/19: 对输出文件数进行自定义=20，且不混洗
 */
public class MergeCompress {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(args[0]);
        textFile.coalesce(20,false).saveAsTextFile(args[1], LzopCodec.class);
        sc.stop();
    }
}
