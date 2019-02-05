package learning_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RDDAction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("actionRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> lines = sc.parallelize(Arrays.asList(1, 2, 3, 4));


        /**
         * action操作:
         * top()
         * take()
         * takeOrdered()
         * countByValue()
         * takeSample()
         */
        List<Integer> top = lines.top(2);
        List<Integer> take = lines.take(1);
        List<Integer> integers = lines.takeOrdered(3,new IntegerComparator());
        Map<Integer, Long> countByValue = lines.countByValue();
        List<Integer> integers2 = lines.takeSample(false, 1);

        /**
         * foreach()
         * reduce()
         * fold()
         * aggregate()可以返回与输入值类型不同的类型
         */
        lines.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        Integer result2 = lines.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });
        Integer result3 = lines.fold(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });

        AvgCount aggregate = lines.aggregate(new AvgCount(0, 0), new AddAndCount(), new Combine());
        System.out.println(aggregate.avg());

        System.out.println(result2.equals(result3));
        /**
         * collect收集所有数据，因此不适合特别大的数据集
         */
        List<Integer> collect = lines.collect();
        System.out.println(collect);
    }

    /**
     * 初始化类
     */
    private static class AvgCount implements Serializable {
        int total;
        int num;

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public double avg() {
            return total / (double) num;
        }
    }

    /**
     * 分区内初始化后添加新元素
     */
    private static class AddAndCount implements Function2<AvgCount, Integer, AvgCount> {
        @Override
        public AvgCount call(AvgCount a, Integer x) throws Exception {
            a.total += x;
            a.num += 1;
            return a;
        }
    }

    /**
     * 分区间数据合并
     */
    private static class Combine implements Function2<AvgCount, AvgCount, AvgCount> {
        @Override
        public AvgCount call(AvgCount a, AvgCount b) throws Exception {
            a.total += b.total;
            a.num += b.num;
            return a;
        }
    }


}
