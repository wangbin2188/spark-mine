import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wangbin10 on 2018/8/2.
 * 用于清洗出用户新增日期和最后活跃日期的表，基于ycapp_active
 * 存储到device_info的user列簇
 */
public class UserFirstAndLast {
    public static final String SOURCE = "hdfs:///";
    public static final String IP = "ip1,ip2";
    public static final String PORT = "2181";
    public static final String TABLE_NAME = "device_info";
    public static final String COLUMN_FAMILY = "user";
    public static final String COLUMN_NAME_ADD = "add_dt";
    public static final String COLUMN_NAME_LAST = "last_dt";
    public static final String COLUMN_NAME_UID = "uid";
    public static final String COLUMN_NAME_CHA = "cha";
    public static final String COLUMN_NAME_OS = "os";
    public static final String COLUMN_NAME_AV = "av";
    public static final String FUTURE = "20700101";
    public static final SimpleDateFormat SDF_1 = new SimpleDateFormat("yyyyMMdd");
    public static final SimpleDateFormat SDF_2 = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {
        JobConf jobConf = getJobConf();
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        /**自定义两个广播变量，当前日期和时间戳
         * 自定义时间戳随时间递减*/
        Broadcast<String> bcDate = sc.broadcast(args[0]);
        Broadcast<Long> bcLong = sc.broadcast(getCustomTimeStamp(args[0]));
        JavaRDD<String> lines = sc.textFile(SOURCE + args[0]);
        JavaPairRDD<ImmutableBytesWritable, Put> result = lines.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(String s) throws Exception {
                String currentDate = bcDate.getValue();
                String formatDate = getFormatDate(currentDate);
                Long ts = bcLong.getValue();
                String[] split = s.split("\001");
                String dvid = split[0];
                String uid = split[1];
                String cha = split[2];
                String os = split[4];
                String av = split[5];
                ImmutableBytesWritable row = new ImmutableBytesWritable(Bytes.toBytes(dvid));
                Put put = new Put(Bytes.toBytes(dvid));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME_ADD), ts, Bytes.toBytes(formatDate));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME_LAST), Bytes.toBytes(formatDate));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME_UID), Bytes.toBytes(uid));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME_CHA), Bytes.toBytes(cha));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME_OS), Bytes.toBytes(os));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME_AV), Bytes.toBytes(av));

                return new Tuple2<ImmutableBytesWritable, Put>(row, put);
            }
        });

        result.saveAsHadoopDataset(jobConf);
        sc.stop();

    }

    private static Long getCustomTimeStamp(String currentDate) {
        Long customTimeStamp = 0L;
        try {
            long futureTime = SDF_1.parse(FUTURE).getTime();
            long nowTime = SDF_1.parse(currentDate).getTime();
            customTimeStamp = futureTime - nowTime;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return customTimeStamp;
    }

    private static JobConf getJobConf() {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", IP);
        hbaseConf.set("hbase.zookeeper.property.clientPort", PORT);

        JobConf jobConf = new JobConf(hbaseConf);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
        return jobConf;
    }

    private static String getFormatDate(String date) throws ParseException {
        Date parse = SDF_1.parse(date);
        return SDF_2.format(parse);
    }


}
