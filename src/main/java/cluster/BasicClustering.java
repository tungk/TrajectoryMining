package cluster;

import model.SnapShot;
import model.SnapshotClusters;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

/**
 * A basic clustering method. Each snapshot is clustered separately.
 * 
 * @author a0048267
 * 
 */
public class BasicClustering implements ClusteringMethod {
    private static final long serialVersionUID = 1273022000870413977L;
    public BasicClustering() {}
    
    @Override
    public JavaRDD<SnapshotClusters> doClustering(JavaRDD<String> input, int eps, int minpts, int M, int pars) {
	JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = input
		.filter(new TupleFilter())
		.mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinor(), pars);
	// Key is the time sequence
	// DBSCAN
	JavaRDD<SnapshotClusters> CLUSTERS = TS_CLUSTERS.map(
		new DBSCANWrapper(eps, minpts, M)).filter(
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
