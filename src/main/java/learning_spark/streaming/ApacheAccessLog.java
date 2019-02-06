package learning_spark.streaming;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 访问日志类，定义类访问日志的格式
 */
public class ApacheAccessLog {
    private static final Logger logger = Logger.getLogger("Access");

    private String ipAddress;
    private String clientIdentd;
    private String userID;
    private String dateTimeString;
    private String method;
    private String endpoint;
    private String protocol;//协议
    private int responseCode;//响应代码
    private long contentSize;

    private ApacheAccessLog(String ipAddress, String clientIdentd, String userID,
                            String dateTime, String method, String endpoint,
                            String protocol, String responseCode,
                            String contentSize) {
        this.ipAddress = ipAddress;
        this.clientIdentd = clientIdentd;
        this.userID = userID;
        this.dateTimeString = dateTime;  // TODO: Parse from dateTime String;
        this.method = method;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.responseCode = Integer.parseInt(responseCode);
        this.contentSize = Long.parseLong(contentSize);
    }

    public static Logger getLogger() {
        return logger;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getClientIdentd() {
        return clientIdentd;
    }

    public void setClientIdentd(String clientIdentd) {
        this.clientIdentd = clientIdentd;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public void setDateTimeString(String dateTimeString) {
        this.dateTimeString = dateTimeString;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public long getContentSize() {
        return contentSize;
    }

    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    private static final String LOG_ENTRY_PATTERN =
            // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    /**
     * 使用正则表达式，将String反序列化成ApacheAccessLog对象
     * @param logline
     * @return
     */
    public static ApacheAccessLog parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ApacheAccessLog(
                m.group(1), m.group(2), m.group(3),
                m.group(4),m.group(5), m.group(6),
                m.group(7),m.group(8), m.group(9));
    }
}
