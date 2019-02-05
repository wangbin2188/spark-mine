package learning_spark.io;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class ObjectFileIO {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Object> objectFile = sc.objectFile(args[0]);
        JavaRDD<Person> map = objectFile.map(new Function<Object, Person>() {
            @Override
            public Person call(Object o) throws Exception {
                return (Person) o;
            }
        });

        map.saveAsObjectFile(args[1]);
    }
}
