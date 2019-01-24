import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.*;

/**
 * Created by wangbin10 on 2019/1/24.
 * 对设备去重，每个设备只保留最多10条信息的参数，类似hive中的collect_set(10)
 */
public class SparkTop10 {

    public static final String FIELD_SEPARATOR = "\t";
    public static final String ITEM_SEPARATOR = ",";
    public static final int COUNT = 10;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(args[0]);
        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split(FIELD_SEPARATOR, 2);
                return new Tuple2<String, String>(split[0], split[1]);
            }
        });

        JavaPairRDD<String, Iterable<String>> groupByKey = pairRDD.groupByKey();
        JavaPairRDD<String, String> mapValues = groupByKey.mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> strings) throws Exception {
                List<HashSet<String>> list = new ArrayList<>();
                for (String string : strings) {
                    String[] split = string.split(FIELD_SEPARATOR);
                    for (int i = 0; i < split.length; i++) {
                        if (list.get(i).size()<= COUNT) {
                            list.get(i).add(split[i]);
                        }
                    }
                }
                StringBuilder sb = new StringBuilder();
                for (HashSet<String> stringHashSet : list) {
                    String string = mergeSetToString(stringHashSet);
                    sb.append(string);
                    sb.append(FIELD_SEPARATOR);
                }
                sb.deleteCharAt(sb.length() - 1);
                return sb.toString();
            }
        });

        JavaRDD<String> map = mapValues.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                StringBuilder sb = new StringBuilder();
                sb.append(tuple2._1);
                sb.append(FIELD_SEPARATOR);
                sb.append(tuple2._2);
                return sb.toString();
            }
        });

        map.saveAsTextFile(args[1]);
    }

    private static String mergeSetToString(Set<String> setS) {
        StringBuilder sb = new StringBuilder();
        for (String set : setS) {
            sb.append(set);
            sb.append(ITEM_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
