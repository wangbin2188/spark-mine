package learning_spark.streaming;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;

/**
 * 主程序入口
 */
public class LogAnalyzerAppMain {
    public static final String WINDOW_LENGTH = "window_length";
    public static final String SLIDE_INTERVAL = "slide_interval";
    public static final String LOGS_DIRECTORY = "logs_directory";
    public static final String OUTPUT_HTML_FILE = "output_html_file";
    public static final String CHECKPOINT_DIRECTORY = "checkpoint_directory";
    public static final String INDEX_HTML_TEMPLATE = "index_html_template";
    public static final String OUTPUT_DIRECTORY = "output_directory";

    private static final Options THE_OPTIONS = createOptions();
    private static Options createOptions() {
        Options options = new Options();

        options.addOption(
                new Option(WINDOW_LENGTH, false, "The window length in seconds"));
        options.addOption(
                new Option(SLIDE_INTERVAL, false, "The slide interval in seconds"));
        options.addOption(
                new Option(LOGS_DIRECTORY, true, "The directory where logs are written"));
        options.addOption(
                new Option(OUTPUT_HTML_FILE, false, "Where to write output html file"));
        options.addOption(
                new Option(CHECKPOINT_DIRECTORY, false, "The checkpoint directory."));
        options.addOption(new Option(INDEX_HTML_TEMPLATE, true,
                "path to the index.html.template file - accessible from all workers"));
        options.addOption(new Option(OUTPUT_DIRECTORY, false, "path to output DSTreams too"));

        return options;
    }

    public static void main(String[] args) throws IOException {
        Flags.setFromCommandLineArgs(THE_OPTIONS, args);

        // Startup the Spark Conf.
        SparkConf conf = new SparkConf()
                .setAppName("A Databricks Reference Application: Logs Analysis with Spark");
        JavaStreamingContext jssc = new JavaStreamingContext(conf,
                Flags.getInstance().getSlideInterval());

        // Checkpointing must be enabled to use the updateStateByKey function & windowed operations.
        jssc.checkpoint(Flags.getInstance().getCheckpointDirectory());

        // This methods monitors a directory for new files to read in for streaming.
        JavaDStream<String> logData = jssc.textFileStream(Flags.getInstance().getLogsDirectory());

        JavaDStream<ApacheAccessLog> accessLogsDStream
                = logData.map(new Functions.ParseFromLogLine()).cache();

        final LogAnalyzerTotal logAnalyzerTotal = new LogAnalyzerTotal();
        final LogAnalyzerWindowed logAnalyzerWindowed = new LogAnalyzerWindowed();

        // Process the DStream which gathers stats for all of time.
        logAnalyzerTotal.processAccessLogs(Flags.getInstance().getOutputDirectory(), accessLogsDStream);

        // Calculate statistics for the last time interval.
        logAnalyzerWindowed.processAccessLogs(Flags.getInstance().getOutputDirectory(), accessLogsDStream);

        // Render the output each time there is a new RDD in the accessLogsDStream.
        final Renderer renderer = new Renderer();
        accessLogsDStream.foreachRDD(new Function<JavaRDD<ApacheAccessLog>, Void>() {
            public Void call(JavaRDD<ApacheAccessLog> rdd) {
                // Call this to output the stats.
                try {
                    renderer.render(logAnalyzerTotal.getLogStatistics(),
                            logAnalyzerWindowed.getLogStatistics());
                } catch (Exception e) {
                }
                return null;
            }
        });

        // Start the streaming server.
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
