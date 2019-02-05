package learning_spark.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

public class HbaseIO {
    public static final String OLD_TABLE_NAME = "app_info_dvid";
    public static final String COLUMN_FAMILY = "car";
    public static final String COLUMN_NAME = "info";
    public static final String NEW_TABLE_NAME = "device_info";
    public static final String NEW_CF = "car";
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "");
        Configuration inputConf = getInputHbaseConf(args, hbaseConf);
        JobConf outputConf = getOutputJobConf(hbaseConf);
        JavaPairRDD<ImmutableBytesWritable, Result> input = sc.newAPIHadoopRDD(inputConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaPairRDD<ImmutableBytesWritable, Put> output = input.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, Put>() {

            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<ImmutableBytesWritable, Result> in) throws Exception {
                ImmutableBytesWritable row = in._1;
                Result result = in._2;

                /**初始化Put对象,填充put*/
                Put put = new Put(row.get());
                return new Tuple2<>(row,put);
            }
        });

        output.saveAsHadoopDataset(outputConf);

    }


    private static Configuration getInputHbaseConf(String[] args, Configuration hbaseConf) throws IOException {
        //创建扫描器
        Scan scan = new Scan();
        Long minTimeStamp = Long.valueOf(args[0]);
        Long maxTimeStamp = Long.valueOf(args[1]);
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME));
        scan.setTimeRange(minTimeStamp, maxTimeStamp);
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String ScanToString = Base64.encodeBytes(proto.toByteArray());
        hbaseConf.set(TableInputFormat.INPUT_TABLE, OLD_TABLE_NAME);
        hbaseConf.set(TableInputFormat.SCAN, ScanToString);
        return hbaseConf;
    }

    private static JobConf getOutputJobConf(Configuration hbaseConf) {
        //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的
        JobConf jobConf = new JobConf(hbaseConf);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, NEW_TABLE_NAME);
        return jobConf;
    }
}
