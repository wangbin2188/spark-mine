package learning_spark.sql;

import learning_spark.io.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.util.ArrayList;

/**
 * 基于JavaBean创建DataFrame
 */
public class DataFrameDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);

        ArrayList<Person> people = new ArrayList<>();
        people.add(new Person("tom", 19));
        JavaRDD<Person> peopleRDD = sc.parallelize(people);
        DataFrame peopleDataFrame = hiveContext.applySchema(peopleRDD, Person.class);
        peopleDataFrame.registerTempTable("people");

    }
}
