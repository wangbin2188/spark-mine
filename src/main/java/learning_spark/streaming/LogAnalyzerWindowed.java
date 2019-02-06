package learning_spark.streaming;

import com.google.common.collect.Ordering;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.Tuple4;

import java.util.Comparator;
import java.util.List;

/**
 * 窗口统计，没有输出到文件，而是选择打印到屏幕
 */
public class LogAnalyzerWindowed {
    private LogStatistics logStatistics;

    public void processAccessLogs(String outDir, JavaDStream<ApacheAccessLog> accessLogsDStream) {
        /**
         * 窗口大小和批次大小
         */
        JavaDStream<ApacheAccessLog> windowDStream = accessLogsDStream.window(
                Flags.getInstance().getWindowLength(),
                Flags.getInstance().getSlideInterval());
        /**
         * 从日志中获取IP地址
         */
        JavaDStream<String> ip = accessLogsDStream.map(
                new Function<ApacheAccessLog, String>() {
                    public String call(ApacheAccessLog entry) {
                        return entry.getIpAddress();
                    }
                });
        /**
         * reduceByWindow对窗口进行reduce操作，获取总的请求次数
         * 需要提供两个函数：归约函数和归约函数逆函数
         * 通过在当前值的基础上增加新窗口和减去老窗口，可以减少计算量
         * 后面的参数是窗口大小和批次大小
         */
        JavaDStream<Long> requestCountRBW = accessLogsDStream.map(new Function<ApacheAccessLog, Long>() {
            public Long call(ApacheAccessLog entry) {
                return 1L;
            }
        }).reduceByWindow(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) {
                return v1 + v2;
            }
        }, new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) {
                return v1 - v2;
            }
        }, Flags.getInstance().getWindowLength(), Flags.getInstance().getSlideInterval());
        requestCountRBW.print();

        /**
         * reduceByKeyAndWindow对窗口和key进行归约，对象必须是pairRDD
         */
        JavaPairDStream<String, Long> ipAddressPairDStream = accessLogsDStream.mapToPair(
                new PairFunction<ApacheAccessLog, String, Long>() {
                    public Tuple2<String, Long> call(ApacheAccessLog entry) {
                        return new Tuple2(entry.getIpAddress(), 1L);
                    }
                });
        JavaPairDStream<String, Long> ipCountDStream = ipAddressPairDStream.reduceByKeyAndWindow(
                // Adding elements in the new slice
                new Function2<Long, Long, Long>() {
                    public Long call(Long v1, Long v2) {
                        return v1 + v2;
                    }
                },
                // Removing elements from the oldest slice
                new Function2<Long, Long, Long>() {
                    public Long call(Long v1, Long v2) {
                        return v1 - v2;
                    }
                },
                Flags.getInstance().getWindowLength(),
                Flags.getInstance().getSlideInterval());
        ipCountDStream.print();
        /**
         * DStream还提供了countByWindow()和countByValueAndWindow()作为对数据进行计数操作的简写
         * countByWindow返回一个表示每个窗口中元素个数的DStream;
         * countByValueAndWindow返回的DStream则包含窗口中具体某个值的个数
         */
        JavaDStream<Long> requestCount = accessLogsDStream.countByWindow(
                Flags.getInstance().getWindowLength(), Flags.getInstance().getSlideInterval());
        JavaPairDStream<String, Long> ipAddressRequestCount = ip.countByValueAndWindow(
                Flags.getInstance().getWindowLength(), Flags.getInstance().getSlideInterval());
        requestCount.print();
        ipAddressRequestCount.print();

        // use a transform for the response code count
        JavaPairDStream<Integer, Long> responseCodeCountTransform = accessLogsDStream.transformToPair(
                new Function<JavaRDD<ApacheAccessLog>, JavaPairRDD<Integer, Long>>() {
                    public JavaPairRDD<Integer, Long> call(JavaRDD<ApacheAccessLog> logs) {
                        return Functions.responseCodeCount(logs);
                    }
                });
        windowDStream.foreachRDD(new Function<JavaRDD<ApacheAccessLog>, Void>() {
            public Void call(JavaRDD<ApacheAccessLog> accessLogs) {
                Tuple4<Long, Long, Long, Long> contentSizeStats =
                        Functions.contentSizeStats(accessLogs);

                List<Tuple2<Integer, Long>> responseCodeToCount =
                        Functions.responseCodeCount(accessLogs)
                                .take(100);

                JavaPairRDD<String, Long> ipAddressCounts =
                        Functions.ipAddressCount(accessLogs);
                List<String> ip = Functions.filterIPAddress(ipAddressCounts)
                        .take(100);

                Object ordering = Ordering.natural();
                Comparator<Long> cmp = (Comparator<Long>) ordering;
                List<Tuple2<String, Long>> topEndpoints =
                        Functions.endpointCount(accessLogs)
                                .top(10, new Functions.ValueComparator<String, Long>(cmp));

                logStatistics = new LogStatistics(contentSizeStats, responseCodeToCount,
                        ip, topEndpoints);
                return null;
            }
        });
    }

    public LogStatistics getLogStatistics() {
        return logStatistics;
    }
}
