package com.company;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class SparkProcessTags {
    private static final List<String> AGE = Arrays.asList("010qqq00", "010qqw00", "010qqe00", "010qqt00", "010qqy00");
    private static final List<String> SEX = Arrays.asList("010qwq00", "010qww00");
    private static final List<String> PARENT = Arrays.asList("010qeq00", "010qew00", "010qee00", "010qer00", "010qet00", "010qey00");
    private static final List<String> MARRIAGE = Arrays.asList("010qtq00", "010qte00");
    private static final List<String> EMOTION = Arrays.asList("010qtu00", "010qty00", "010qtt00");
    private static final String PLACE_HOLDER = "-";
    private static final String SEPARATOR = ",";
    public static final String FIELD_SEPARATOR = "\t";


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile(args[0]);
        Properties properties = loadData(args[2]);
        Broadcast<Properties> broadcast = sparkContext.broadcast(properties);
        JavaRDD<String> result = rdd.map( s -> {
            JSONObject json = JSONObject.parseObject(s);
            String error_code = json.getString("error_code");
            String userId = json.getString("userId");
            JSONArray tags = json.getJSONArray("tags");
            Properties props = broadcast.getValue();
            StringBuilder age1 = new StringBuilder();
            StringBuilder sex1 = new StringBuilder();
            StringBuilder parent1 = new StringBuilder();
            StringBuilder marriage1 = new StringBuilder();
            StringBuilder emotion1 = new StringBuilder();
            for (int i = 0; i < tags.size(); i++) {
                JSONObject codes = tags.getJSONObject(i);
                String code = codes.getString("code");
                String codeValue = props.getProperty(code);
                if (AGE.contains(code)) {
                    age1.append(codeValue);
                    age1.append(SEPARATOR);
                } else if (SEX.contains(code)) {
                    sex1.append(codeValue) ;
                    sex1.append(SEPARATOR);
                } else if (PARENT.contains(code)) {
                    parent1.append(codeValue) ;
                    parent1.append(SEPARATOR);
                } else if (MARRIAGE.contains(code)) {
                    marriage1.append(codeValue);
                    marriage1.append(SEPARATOR);
                } else if (EMOTION.contains(code)) {
                    emotion1.append(codeValue);
                    emotion1.append(SEPARATOR);
                }
            }
            List<String> strList = Arrays.asList(error_code, userId, age1.toString(), sex1.toString(), parent1.toString(), marriage1.toString(), emotion1.toString());
            return mergeStr(strList);
        });
        result.saveAsTextFile(args[1]);
    }

    private static Properties loadData(String path) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    /**
     * 合并字段为一个String
     *列分隔符为\t
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

    /**
     * 如果sb为空，更新为“-”，否则去掉最后的逗号
     * @param sb
     * @return
     */
    public static String processStr(StringBuilder sb) {
        if (sb.length() == 0) {
            return PLACE_HOLDER;
        } else {
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }
    }
}
