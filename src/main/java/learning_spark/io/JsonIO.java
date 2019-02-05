package learning_spark.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

public class JsonIO {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * 读取并解析json数据
         * 处理json数据
         * 序列号json数据
         * 保存json数据
         */
        JavaRDD<String> input = sc.textFile("file.json");
        JavaRDD<Person> result = input.mapPartitions(new ParseJson());
//        result.map()
        JavaRDD<String> javaRDD = result.mapPartitions(new WriteJson());
        javaRDD.saveAsTextFile(args[1]);
    }
}

class Person{}
class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
    @Override
    public Iterable<Person> call(Iterator<String> lines) throws Exception {
        ArrayList<Person> people = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        while (lines.hasNext()) {
            String line = lines.next();
            people.add(mapper.readValue(line,Person.class));
        }
        return people;
    }
}

class WriteJson implements FlatMapFunction<Iterator<Person>,String> {
    @Override
    public Iterable<String> call(Iterator<Person> people) throws Exception {
        ArrayList<String> text = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        while (people.hasNext()) {
            Person person = people.next();
            text.add(mapper.writeValueAsString(person));
        }
        return text;
    }
}