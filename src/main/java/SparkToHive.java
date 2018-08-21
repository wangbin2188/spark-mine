import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by wangbin10 on 2018/7/23.
 */
public class SparkToHive {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "AppBaseLogToHive", conf);
        HiveContext hiveContext = new HiveContext(sc);
        DataFrame df = hiveContext.sql("use database;select * from table");
        df.write().saveAsTable("tablename");
        df.saveAsTable("tablename","overwrite");


    }
}
