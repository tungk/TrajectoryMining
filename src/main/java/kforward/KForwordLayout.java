package kforward;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import conf.AppProperties;
import model.SnapShot;

public class KForwordLayout implements Serializable {
    private static final long serialVersionUID = 5312596867491192993L;

    private JavaRDD<String> input;

    private final int K, L, G, M; // these parameters are re-assigned locally
    private final int SYS_PARTITIONS; // need to know how many partitions the
				      // system supports

    public KForwordLayout(JavaRDD<String> input) {
	K = Integer.parseInt(AppProperties.getProperty("K"));
	L = Integer.parseInt(AppProperties.getProperty("L"));
	M = Integer.parseInt(AppProperties.getProperty("M"));
	G = Integer.parseInt(AppProperties.getProperty("G"));
	SYS_PARTITIONS = Integer.parseInt(AppProperties
		.getProperty("kforward_partitions"));
	this.input = input;
    }

    public JavaPairRDD<Integer, Iterable<HashSet<Integer>>> runLogic() {
	// DBSCAN preparation, we need SnapShot since it contains object
	// location
	JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = input
		.filter(new TupleFilter())
		.mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinor(),
			conf.Constants.SNAPSHOT_PARTITIONS);

	// DBSCAN
	JavaRDD<ArrayList<SimpleCluster>> CLUSTERS = TS_CLUSTERS
		.map(new DBSCANWrapper(conf.Constants.EPS,
			conf.Constants.MINPTS, M)).filter(
			new Function<ArrayList<SimpleCluster>, Boolean>() {
			    private static final long serialVersionUID = 7146570874034097868L;

			    @Override
			    public Boolean call(ArrayList<SimpleCluster> v1)
				    throws Exception {
				return v1 != null && v1.size() > 0;
			    }
			});
	// TODO:: temporally hard-code first for efficiency
	int min_ts = 2;
	int max_ts = 2879; 

	System.out.println("min:" + min_ts);
	System.out.println("max:" + max_ts);

	// do the k-forward
	JavaPairRDD<Integer, Iterable<HashSet<Integer>>> candidate_w_duplicate = CLUSTERS
		.flatMapToPair(
			new KForwardPartitioner(min_ts, max_ts, K, SYS_PARTITIONS))
		.groupByKey()
		.mapValues(new LocalPattern(L, K, M, G))
		.mapValues(
			new Function<Iterable<HashSet<Integer>>, Iterable<HashSet<Integer>>>() {
			    private static final long serialVersionUID = -4091989638424505298L;
			    @Override
			    public Iterable<HashSet<Integer>> call(
				    Iterable<HashSet<Integer>> v1)
				    throws Exception {
				// TODO:: do a local duplicate removal
				return v1;
			    }
			});
	// result.cache();
	return candidate_w_duplicate;
    }
}
