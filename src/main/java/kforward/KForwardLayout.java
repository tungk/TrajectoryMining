package kforward;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import conf.AppProperties;
import model.SnapShot;
import model.SnapshotClusters;

public class KForwardLayout implements Serializable {
    private static final long serialVersionUID = 5312596867491192993L;

    private JavaRDD<SnapshotClusters> clusters;

    private final int K, L, G, M; // these parameters are re-assigned locally
    private final int SYS_PARTITIONS; // need to know how many partitions the
				      // system supports

    public KForwardLayout(int k, int l, int m, int g) {
	K = k;
	L = l;
	M = m;
	G = g;
	SYS_PARTITIONS = Integer.parseInt(AppProperties
		.getProperty("kforward_partitions"));
    }
    
    public void setInput(JavaRDD<SnapshotClusters> input) {
	clusters = input;
    }

    public JavaPairRDD<Integer, ArrayList<HashSet<Integer>>> runLogic() {
	// TODO:: temporally hard-code first for efficiency
	int min_ts = 2;
	int max_ts = 2879; 

	// do the k-forward
	JavaPairRDD<Integer, ArrayList<HashSet<Integer>>> candidate_w_duplicate = clusters
		.flatMapToPair(
			new KForwardPartitioner(min_ts, max_ts, K, SYS_PARTITIONS))
		.groupByKey()
		.mapValues(new LocalPattern(L, K, M, G))
		.mapValues(
			new Function<ArrayList<HashSet<Integer>>, ArrayList<HashSet<Integer>>>() {
			    private static final long serialVersionUID = -4091989638424505298L;
			    @Override
			    public ArrayList<HashSet<Integer>> call(
				    ArrayList<HashSet<Integer>> v1)
				    throws Exception {
				// TODO:: do a local duplicate removal
				return v1;
			    }
			});
	// result.cache();
	return candidate_w_duplicate;
    }
}
