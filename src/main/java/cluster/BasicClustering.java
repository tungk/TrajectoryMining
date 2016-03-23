package cluster;

import java.io.Serializable;


import model.SnapShot;
import model.SnapshotClusters;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import conf.Constants;

/**
 * A basic clustering method. Each snapshot is clustered separately.
 * 
 * @author a0048267
 * 
 */
public class BasicClustering implements ClusteringMethod {
    private static final long serialVersionUID = 1273022000870413977L;
    public BasicClustering() {}

    public JavaRDD<SnapshotClusters> doClustering(JavaRDD<String> input, int M) {
	JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = input
		.filter(new TupleFilter())
		.mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinor(),
			Constants.SNAPSHOT_PARTITIONS);
	// Key is the time sequence
	// DBSCAN
	JavaRDD<SnapshotClusters> CLUSTERS = TS_CLUSTERS.map(
		new DBSCANWrapper(Constants.EPS, Constants.MINPTS, M)).filter(
		new Function<SnapshotClusters, Boolean>() {
		    private static final long serialVersionUID = 7146570874034097868L;
		    @Override
		    public Boolean call(SnapshotClusters v1) throws Exception {
			return v1 != null && v1.getClusterSize() > 0;
		    }
		});
	return CLUSTERS;
    }
}
