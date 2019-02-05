package learning_spark;

import org.apache.spark.Partitioner;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * 自定义 partitioner
 *
 */
public class DomainNamePartitioner extends Partitioner {
    int numParts;

    public DomainNamePartitioner() {
        this(20);
    }

    public DomainNamePartitioner(int numParts) {
        this.numParts = numParts;
    }

    @Override
    public int numPartitions() {
        return numParts;
    }

    @Override
    public int getPartition(Object o) {
        String key = o.toString();
        String host = null;
        try {
            host = new URL(key).getHost();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return Math.abs(host.hashCode()) % numParts;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
