package learning_spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

public class UDFDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);

        UDFRegistration udf = hiveContext.udf();
        /**
         * spark udf函数定义并注册
         */
        udf.register("stringLength", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);

        DataFrame dataFrame = hiveContext.sql("select stringLength(name) from tweets limit 10");
        Row[] collect = dataFrame.collect();
        for (Row row : collect) {
            System.out.println(row.get(0));
        }

    }
}
