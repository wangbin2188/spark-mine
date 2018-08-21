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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by wangbin10 on 2018/7/25.
 * 从ycapp_address表清洗数据到device_info的user列簇
 */
public class UserAddress {
    public static final String SOURCE = "hdfs:///";
    public static final String IP = "ip1,ip2";
    public static final String PORT = "2181";
    public static final String TABLE_NAME = "device_info";
    public static final String COLUMN_FAMILY = "user";
    public static final String COLUMN_NAME_PROVINCE = "province";
    public static final String COLUMN_NAME_CITY = "cityname";

    public static void main(String[] args) {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", IP);
        hbaseConf.set("hbase.zookeeper.property.clientPort", PORT);

        //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的
        JobConf jobConf = new JobConf(hbaseConf);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(SOURCE + args[0]);
        JavaPairRDD<String, String> pairRDD = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split("\001");
                return new Tuple2<String, String>(split[0], split[1]);
            }
        });

        JavaPairRDD<String, String> filter = pairRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringTuple2) throws Exception {
                String address = stringTuple2._2;
                if (!address.startsWith("中国") || address.length()<=5) {
                    return false;
                } else {
                    return true;
                }
            }
        });

        JavaPairRDD<String, Tuple2<String, String>> provinceCity = filter.mapValues(new Function<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                //这里截掉了中国两个字符
                String provinceStr = s.substring(2);
                Character char2 = provinceStr.charAt(2);
                String province;
                String cityStr;
                if (provinceStr.startsWith("北京市")) {
                    province="北京";
                    cityStr = "北京";
                } else if (provinceStr.startsWith("天津市")) {
                    province="天津";
                    cityStr = "天津";
                } else if (provinceStr.startsWith("上海市")) {
                    province="上海";
                    cityStr = "上海";
                } else if (provinceStr.startsWith("重庆市")) {
                    province="重庆";
                    cityStr = "重庆";
                } else if (provinceStr.startsWith("香港特别行政区")) {
                    province="香港";
                    cityStr = "香港";
                } else if (provinceStr.startsWith("澳门特别行政区")) {
                    province="澳门";
                    cityStr = "澳门";
                } else if(provinceStr.startsWith("内蒙古自治区")){
                    province="内蒙古";
                    cityStr=provinceStr.substring(6);
                }else if(provinceStr.startsWith("广西壮族自治区")){
                    province="广西";
                    cityStr = provinceStr.substring(7);
                }else if(provinceStr.startsWith("西藏自治区")){
                    province="西藏";
                    cityStr = provinceStr.substring(5);
                }else if(provinceStr.startsWith("宁夏回族自治区")){
                    province="宁夏";
                    cityStr = provinceStr.substring(7);
                }else if(provinceStr.startsWith("新疆维吾尔自治区")){
                    province="新疆";
                    cityStr = provinceStr.substring(8);
                }else if(provinceStr.startsWith("黑龙江省")){
                    province="黑龙江";
                    cityStr=provinceStr.substring(4);
                }else if(char2.equals('省')){
                    province=provinceStr.substring(0,2);
                    cityStr = provinceStr.substring(3);
                }else{
                    //部分地址信息不规范，不带‘省’，则取前两个字为省份，之后为城市名
                    province=provinceStr.substring(0,2);
                    cityStr=provinceStr.substring(2);
                }
                String[] citySplit = cityStr.split("市|地区|盟|自治州");
                return new Tuple2<String, String>(province,citySplit[0]);
            }
        });

        JavaPairRDD<ImmutableBytesWritable, Put> result = provinceCity.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Tuple2<String, String>> tuple2) throws Exception {
                String dvid = tuple2._1;
                ImmutableBytesWritable rowKey=new ImmutableBytesWritable(Bytes.toBytes(dvid));
                Tuple2<String, String> tuple21 = tuple2._2;
                Put put=new Put(Bytes.toBytes(dvid));
                put.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME_PROVINCE), Bytes.toBytes(tuple21._1));
                put.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_NAME_CITY), Bytes.toBytes(tuple21._2));
                return new Tuple2<ImmutableBytesWritable, Put>(rowKey,put);
            }
        });

        result.saveAsHadoopDataset(jobConf);
        sc.stop();
    }
}
