import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by wangbin10 on 2018/8/14.
 */
public class SparkMysql {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(args[0]);
        textFile.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> strs) throws Exception {
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = null;
                PreparedStatement statement = null;
                String sql = "insert into app_client(dt,c_key,c_value) values(?,?,?)";
                conn = DriverManager.getConnection("jdbc:mysql://dbip:dbport/databoard", "dbuser", "dbpw");
                statement = conn.prepareStatement(sql);
                while (strs.hasNext()) {
                    String next = strs.next();
                    String[] split = next.split("\t");
                    Map<String, Object> map = new HashMap<>();
                    map.put("show", split[2]);
                    map.put("click", split[3]);
                    JSONObject json = new JSONObject(map);
                    statement.setInt(1, Integer.valueOf(split[0]));
                    statement.setString(2, split[1]);
                    statement.setString(3, json.toString());
                    statement.executeUpdate();
                }
            }
        });
    }
}
