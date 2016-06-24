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

    /**
     * we don't want to repeatedly "new" this function when use.
     */
    private static final Function<SnapshotClusters, Boolean> filterFunction = new Function<SnapshotClusters, Boolean>() {
	private static final long serialVersionUID = 7146570874034097868L;
	@Override
	public Boolean call(SnapshotClusters v1) throws Exception {
	    if (v1 != null && v1.getClusterSize() > 0) {
		return true;
	    } else {
		return false;
	    }
	}
    };
    
    private static final SnapshotGenerator ssg = new SnapshotGenerator();
    private static final SnapshotCombinor ssc = new SnapshotCombinor();
    private static final TupleFilter tf = new TupleFilter();
    
    private DBSCANWrapper dwr;
    private int pars;
    public BasicClustering(int eps, int minpts, int M, int pars, int earth) {
	this.pars = pars;
	dwr = new DBSCANWrapper(eps, minpts, M, earth);
    }

    @Override
    public JavaRDD<SnapshotClusters> doClustering(JavaRDD<String> input) {
	JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = input
		.filter(tf).mapToPair(ssg)
		.reduceByKey(ssc, pars);
	// Key is the time sequence
	// DBSCAN
	JavaRDD<SnapshotClusters> CLUSTERS = TS_CLUSTERS.map(
		dwr).filter(filterFunction);
	return CLUSTERS;
    }
}
