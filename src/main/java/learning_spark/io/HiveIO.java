package learning_spark.io;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class HiveIO {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);
        DataFrame rows = hiveContext.sql("select name,age from users");
        Row first = rows.first();
        System.out.println(first.get(0));

        /**
         * hiveContext也可以读取json数据
         */
        DataFrame jsonFile = hiveContext.jsonFile(args[0]);
        jsonFile.registerTempTable("users2");
        DataFrame frame = hiveContext.sql("select name,text from users2");
    }
}
