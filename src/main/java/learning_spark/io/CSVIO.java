package learning_spark.io;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.StringReader;


public class CSVIO {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> csvFile = sc.textFile(args[0]);
        JavaRDD<String[]> csvData = csvFile.map(new ParseLine());

        JavaPairRDD<String, String> csvFile2 = sc.wholeTextFiles(args[0]);
        JavaRDD<String[]> csvData2 = csvFile2.flatMap(new ParseLine2());

        csvData.saveAsTextFile(args[1]);

    }
}

/**
 * 如果csv所有数据字段均没有包含换行符，可以使用textFile读取并解析数据
 */
class ParseLine implements Function<String, String[]> {

    @Override
    public String[] call(String line) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(line));
        return reader.readNext();
    }
}

/**
 * 如果字段中嵌有换行符，就需要完整读入每个文件，然后解析各段
 */
class ParseLine2 implements FlatMapFunction<Tuple2<String,String>, String[]> {

    @Override
    public Iterable<String[]> call(Tuple2<String,String> file) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(file._2()));
        return reader.readAll();
    }
}

