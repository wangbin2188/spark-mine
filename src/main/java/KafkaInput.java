/**
 * Illustrates a simple map then filter in Java
 * 更新内容：
 * 1.catch Exception增加日志
 * 2.修改create_date解析方法，向后调整8 hour
 * 3.增加checkPoint机制
 * 4.增加stopByMarkFile关闭机制
 */

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class KafkaInput {
    public static void main(String[] args) throws Exception {
        String zkQuorum = "ip:2181,ip:2181,ip:2181";
        String group = "dataGroupCluster";
        SparkConf conf = new SparkConf();
        conf.setAppName("KafkaInput");
        //优雅的关闭
        conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        // 指定每个批次的处理时间间隔,先只取一个topic的内容
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(Integer.valueOf(args[1])));
        String checkPointPath = "hdfs://";
        jssc.checkpoint(checkPointPath);
        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("topic1", 3);
        

        JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, zkQuorum, group, topics);
        Logger logger = input.dstream().log();
        JavaDStream<String> originStr = input.map(new Function<Tuple2<String, String>, String>() {
            /**取出元组中的有效字符串，去掉所有空白字符后，解析成json*/
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return replaceBlank(stringStringTuple2._2);
            }
        });

        /**去掉解析失败和异常的记录,无需考虑useraction,已无相关日志*/
        JavaDStream<String> filterStr = originStr.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.contains("NSJSONSerialization")) {
                    logger.info(s);
                    return false;
                }
                try {
                    JSONObject originJson = JSONObject.parseObject(s);
                    String message = originJson.getString("Message");
                    JSONObject messageJson = (JSONObject) JSONObject.parse(message);
                    String data = messageJson.getString("data");
                    JSONObject dataJson = (JSONObject) JSONObject.parse(data);
                    JSONArray list = dataJson.getJSONArray("list");
                } catch (JSONException e) {
                    e.printStackTrace();
                    logger.info(s, e);
                    return false;
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    logger.info(s, e);
                    return false;
                }
                return true;
            }
        });
        /**将过滤后的字符串解析成json*/
        JavaDStream<JSONObject> filterJson = filterStr.map(new Function<String, JSONObject>() {
            @Override
            public JSONObject call(String s) throws Exception {
                return JSONObject.parseObject(s);
            }
        });

        JavaDStream<String> finalStr = filterJson.flatMap(new FlatMapFunction<JSONObject, String>() {
            @Override
            public Iterable<String> call(JSONObject jsonObject) throws Exception {
                /**从顶层Json获取createDate,level,message*/
                String createDate = null;
                String level = null;
                String s_ip = null;
                String s_ver = null;
                String s_ua = null;
                String s_action = null;
                String s_method = null;
                String s_control = null;
                String s_host = null;
                String lau = null;
                String av = null;
                String srh = null;
                String nt = null;
                String dvid = null;
                String yna = null;
                String osv = null;
                String imei = null;
                String srw = null;
                String os = null;
                String cha = null;
                String idfa = null;
                String uid = null;
                String mdl = null;
                String no = null;
                String fac = null;
                String lou = null;
                String sc = null;
                String na = null;
                String cyid = null;
                String ifda = null;
                JSONArray list = null;
                try {
                    createDate = parseDate(jsonObject.getString("CreateDate"));
                    level = jsonObject.getString("Level");
                    String message = jsonObject.getString("Message");
                    /**从message获取data，s_ip等数据*/
                    JSONObject messageJson = (JSONObject) JSONObject.parse(message);
                    String data = messageJson.getString("data");
                    s_ip = messageJson.getString("s_ip");
                    s_ver = messageJson.getString("s_ver");
                    s_ua = messageJson.getString("s_ua");
                    s_action = messageJson.getString("s_action");
                    s_method = messageJson.getString("s_method");
                    s_control = messageJson.getString("s_control");
                    s_host = messageJson.getString("s_host");
                    /**
                     * 从data获取用户手机硬件和APP的基本信息, dvid,os,av,uid,cyid和用户行为的list
                     * 注意经纬度信息是Double类型
                     * */
                    JSONObject dataJson = (JSONObject) JSONObject.parse(data);
                    lau = dataJson.getString("lau");
                    av = dataJson.getString("av");
                    srh = dataJson.getString("srh");
                    nt = dataJson.getString("nt");
                    dvid = dataJson.getString("dvid");
                    yna = dataJson.getString("yna");
                    osv = dataJson.getString("osv");
                    imei = dataJson.getString("imei");
                    srw = dataJson.getString("srw");
                    os = dataJson.getString("os");
                    cha = dataJson.getString("cha");
                    idfa = dataJson.getString("idfa");
                    uid = dataJson.getString("uid");
                    mdl = dataJson.getString("mdl");
                    no = dataJson.getString("no");
                    fac = dataJson.getString("fac");
                    lou = dataJson.getString("lou");
                    sc = dataJson.getString("sc");
                    na = dataJson.getString("na");
                    cyid = dataJson.getString("cyid");
                    ifda = dataJson.getString("ifda");
                    list = dataJson.getJSONArray("list");
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info(jsonObject.toJSONString(), e);
                }
                /**
                 * 从用户行为list获取事件的基本信息type,dt等*/
                List<String> kvList = new ArrayList<>();
                for (int i = 0; i < list.size(); i++) {
                    JSONObject event = list.getJSONObject(i);
                    String type = event.getString("type");
                    List<String> strList = Arrays.asList(createDate, level, type, lau, av, srh, nt, dvid, yna, osv, imei, srw, os, cha, idfa, uid, mdl, no, fac, lou, sc, na, cyid, ifda, s_ip, s_ver, s_ua, s_action, s_method, s_control, s_host, event.toJSONString());
                    String varStr = mergeStr(strList);
                    kvList.add(varStr);
                }
                return kvList;
            }
        });


        finalStr.dstream().saveAsTextFiles(args[0], "");
        jssc.start();
        stopByMarkFile(jssc);
    }

    /**
     * 替换空白字符
     */
    public static String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }

    /**
     * 合并字段为一个String
     *
     * @param
     */
    public static String mergeStr(List<String> strs) {
        StringBuilder sb = new StringBuilder();
        for (String str : strs) {
            sb.append(str);
            sb.append("\t");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    /**
     * 解析createDate
     */
    public static String parseDate(String dateStr) {
        String nStr = dateStr.replaceAll("T|Z", " ");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ");
        Date parse = null;
        try {
            parse = sdf.parse(nStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        /**调整为8小时后的时间*/
        long time = parse.getTime() + 8 * 60 * 60 * 1000;
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf2.format(time);
    }

    /**
     * 当标识文件存在时，停止任务
     */
    public static void stopByMarkFile(JavaStreamingContext jssc) throws IOException {
        long time = 10 * 1000;
        boolean isStop = false;
        String path = "hdfs://";
        while (!isStop) {
            /**awaitTerminationOrTimeout
             * 等待time时间后停止，如果停止返回true，否则false*/
            isStop = jssc.awaitTerminationOrTimeout(time);
            if (!isStop && isExistFile(path)) {
                jssc.stop(true, true);
            }
        }
    }

    public static boolean isExistFile(String path) throws IOException {
        Configuration configuration = new Configuration();
        Path hdfsPath = new Path(path);
        FileSystem fs = hdfsPath.getFileSystem(configuration);
        return fs.exists(hdfsPath);
    }

}
