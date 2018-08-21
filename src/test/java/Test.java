import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wangbin10 on 2018/8/1.
 */
public class Test {
    public static void main(String[] args) throws ParseException {
        String format = getFormatDate("20180101");
        System.out.println(format);

    }

    private static String getFormatDate(String sdate) throws ParseException {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2=new SimpleDateFormat("yyyy-MM-dd");
        Date parse = sdf.parse(sdate);
        return sdf2.format(parse);
    }
}
