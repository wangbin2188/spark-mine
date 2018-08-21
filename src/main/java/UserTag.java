import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangbin10 on 2018/7/31.
 * 从用户主动标签表app_info_dvid清洗数据到device_info的car列簇
 */
public class UserTag {

    public static final String ZK_IP = "ip1,ip2,ip3";
    public static final String ZK_PORT = "2181";
    public static final String OLD_TABLE_NAME = "app_info_dvid";
    public static final String COLUMN_FAMILY = "car";
    public static final String COLUMN_NAME = "info";
    public static final String NEW_TABLE_NAME = "device_info";
    public static final String NEW_CF = "car";
    public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", ZK_IP);
        hbaseConf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        Configuration inputConf = getInputHbaseConf(args, hbaseConf);
        JobConf outputConf = getOutputJobConf(hbaseConf);

        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaPairRDD<ImmutableBytesWritable, Result> input = sc.newAPIHadoopRDD(inputConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaPairRDD<ImmutableBytesWritable, Put> output = input.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<ImmutableBytesWritable, Result> in) throws Exception {
                ImmutableBytesWritable row = in._1;
                Result result = in._2;

                /**初始化Put对象*/
                Put put = new Put(row.get());

                Map<String, Object> result1 = getResult(result);
                /**处理时间戳，解析为可直接使用的日期+时间格式*/
                Long dt = (Long) result1.get("dt");
                String parseDt = SDF.format(new Date(dt));
                put.addColumn(Bytes.toBytes(NEW_CF), Bytes.toBytes("dt"), Bytes.toBytes(parseDt));

                String str = (String) result1.get("car:info");
                JSONArray infoArray = JSONArray.parseArray(str);
                String haveCar;
                String buyCarPlan;
                String carPrice;
                String buyCarMethod;
                String favoriteCarModel;
                String ifReplace;
                if (infoArray != null && infoArray.size() != 0) {
                    for (int i = 0; i < infoArray.size(); i++) {
                        JSONObject jsonObject = infoArray.getJSONObject(i);
                        String title = jsonObject.getString("title");
                        if (title.contains("您目前的情况")) {
                            haveCar = jsonObject.getString("content");
                            put.addColumn(Bytes.toBytes(NEW_CF), Bytes.toBytes("haveCar"), Bytes.toBytes(haveCar));
                        }
                        if (title.contains("购车计划")) {
                            buyCarPlan = jsonObject.getString("content");
                            put.addColumn(Bytes.toBytes(NEW_CF), Bytes.toBytes("buyCarPlan"), Bytes.toBytes(buyCarPlan));
                        }
                        if (title.contains("价位")) {
                            carPrice = jsonObject.getString("content");
                            put.addColumn(Bytes.toBytes(NEW_CF), Bytes.toBytes("carPrice"), Bytes.toBytes(carPrice));
                        } else if (title.contains("购车方式是")) {
                            buyCarMethod = jsonObject.getString("content");
                            put.addColumn(Bytes.toBytes(NEW_CF), Bytes.toBytes("buyCarMethod"), Bytes.toBytes(buyCarMethod));
                        }
                        if (title.contains("车型")) {
                            favoriteCarModel = jsonObject.getString("content");
                            put.addColumn(Bytes.toBytes(NEW_CF), Bytes.toBytes("favoriteCarModel"), Bytes.toBytes(favoriteCarModel));
                        }
                        if (title.contains("置换")) {
                            ifReplace = jsonObject.getString("content");
                            put.addColumn(Bytes.toBytes(NEW_CF), Bytes.toBytes("ifReplace"), Bytes.toBytes(ifReplace));
                        }
                    }
                }

                return new Tuple2<ImmutableBytesWritable, Put>(row, put);
            }
        });

        output.saveAsHadoopDataset(outputConf);
        sc.stop();
    }

    private static JobConf getOutputJobConf(Configuration hbaseConf) {
        //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的
        JobConf jobConf = new JobConf(hbaseConf);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, NEW_TABLE_NAME);
        return jobConf;
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

    private static Map<String, Object> getResult(Result result) {
        Map<String, Object> map = new HashMap();
        Cell[] cells = result.rawCells();
        byte[] row1 = result.getRow();
        for (Cell cell : cells) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] column = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            long timestamp = cell.getTimestamp();
            map.put(Bytes.toString(family) + ":" + Bytes.toString(column), Bytes.toString(value));
            map.put("dt", timestamp);
        }
        map.put("dvid", Bytes.toString(row1));
        return map;
    }
}
