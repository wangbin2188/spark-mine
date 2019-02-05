package learning_spark;

import java.util.Comparator;

/**
 * 自定义Comparator
 * rdd.sortByKey(new learning_spark.IntegerComparator())
 */
public class IntegerComparator implements Comparator<Integer> {
    @Override
    public int compare(Integer o1, Integer o2) {
        return Integer.compare(o2,o1);
    }
}
