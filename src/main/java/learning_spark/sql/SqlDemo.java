package learning_spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * DataFrame --> RDD
 */
public class SqlDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);
        DataFrame input = hiveContext.jsonFile(args[0]);
        input.registerTempTable("tweets");
        DataFrame topTweets = hiveContext.sql("select text,count from tweets limit 10");
        JavaRDD<Row> rowJavaRDD = topTweets.toJavaRDD();
        rowJavaRDD.map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        });

    }
}
