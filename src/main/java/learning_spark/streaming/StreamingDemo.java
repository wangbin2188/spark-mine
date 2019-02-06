package learning_spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 7777);
        JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("error");
            }
        });

        errorLines.print();
        jsc.start();
        jsc.awaitTermination();

    }
}
