import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkTags {

    public static final String FIELD_SEPARATOR = "\t";
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile(args[0]);
        JavaRDD<String> filter = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                JSONObject json = JSONObject.parseObject(s);
                Integer error_code = json.getInteger("error_code");
                if (error_code.equals(0)) {
                    return true;
                }
                return false;
            }
        });
        JavaRDD<String> result = filter.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                JSONObject json = JSONObject.parseObject(s);
                String userId = json.getString("userId");
                JSONArray tags = json.getJSONArray("tags");

                ArrayList<String> strings = new ArrayList<>();
                for (int i = 0; i < tags.size(); i++) {
                    JSONObject codes = tags.getJSONObject(i);
                    String code = codes.getString("code");
                    String weight = null;
                    if (codes.containsKey("weight")) {
                        weight = codes.getString("weight");
                    } else {
                        weight="-1";
                    }
                    List<String> stringList = Arrays.asList(userId, code, weight);
                    String str = mergeStr(stringList);
                    strings.add(str);
                }
                return strings;
            }
        });

        result.saveAsTextFile(args[1]);
    }



    /**
     * 合并字段为一个String
     * 列分隔符为\t
     *
     * @param
     */
    private static String mergeStr(List<String> strS) {
        StringBuilder sb = new StringBuilder();
        for (String str : strS) {
            sb.append(str);
            sb.append(FIELD_SEPARATOR);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }


}
