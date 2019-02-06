package learning_spark.streaming;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

public class ReadTransferStats implements Serializable {
    public JavaPairDStream<Long, Integer> readStats(JavaStreamingContext jssc, String inputDirectory) {
        // Note: This example doesn't work until Spark 1.2
        JavaPairDStream<LongWritable, Text> input =
                jssc.fileStream(inputDirectory,
                        LongWritable.class,
                        Text.class,
                        TextInputFormat.class);

        JavaPairDStream<Long, Integer> usefulInput =
                input.mapToPair(new PairFunction<Tuple2<LongWritable, Text>, Long, Integer>() {
                    public Tuple2<Long, Integer> call(Tuple2<LongWritable, Text> input) {
                        return new Tuple2(input._1().get(), Integer.parseInt(input._2().toString()));
                    }
                });
        return usefulInput;
    }
}
