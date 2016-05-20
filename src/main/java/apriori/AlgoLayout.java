package apriori;

import java.io.Serializable;
import it.unimi.dsi.fastutil.ints.IntSet;
import model.SnapshotClusters;

import org.apache.spark.api.java.JavaRDD;

public interface AlgoLayout extends Serializable {
    public void setInput(JavaRDD<SnapshotClusters> CLUSTERS);
    public JavaRDD<IntSet> runLogic();
}
