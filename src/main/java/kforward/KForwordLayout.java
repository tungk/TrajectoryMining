package kforward;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import conf.AppProperties;
import model.Cluster;
import model.SnapShot;

public class KForwordLayout implements Serializable {
    private static final long serialVersionUID = 5312596867491192993L;

    private JavaRDD<String> input;

    private final int K, L, G, M; // these parameters are re-assigned locally
    private final int SYS_PARTITIONS;

    public KForwordLayout(JavaRDD<String> input) {
	K = Integer.parseInt(AppProperties.getProperty("K"));
	L = Integer.parseInt(AppProperties.getProperty("L"));
	M = Integer.parseInt(AppProperties.getProperty("M"));
	G = Integer.parseInt(AppProperties.getProperty("G"));
	SYS_PARTITIONS = Integer.parseInt(AppProperties.getProperty("kforward_partitions"));
	this.input =input;
    }

    public void runLogic() {
	// DBSCAN first
	JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = input
		.filter(new TupleFilter()).mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinor(),
			conf.Constants.SNAPSHOT_PARTITIONS);
	// DBSCAN
	JavaRDD<SnapshotCluster> CLUSTERS = TS_CLUSTERS
		.map(new DBSCANWrapper(conf.Constants.EPS,
			conf.Constants.MINPTS))
		.filter(new Function<ArrayList<Cluster>, Boolean>(){
		    private static final long serialVersionUID = 7146570874034097868L;
		    //remove empty snapshots
		    @Override
		    public Boolean call(ArrayList<Cluster> v1) throws Exception {
			return v1.size() > 0;
		    }
		})
		.map(new TempoClusters(M));
	
	//a centralized step to count max and min timestamp in the given dataset
	SerialSnapshotClusterComp ssc = new SerialSnapshotClusterComp();
//	int min_ts = CLUSTERS.min(ssc).getTS();
//	int max_ts = CLUSTERS.max(ssc).getTS();
	int min_ts = 2; int max_ts = 2879; //hard-code first for efficiency
	
	System.out.println("min:"  + min_ts);
	System.out.println("max:"  + max_ts);
	
	//do the k-forward
	JavaPairRDD<Integer, Iterable<Set<Integer>>> 
	result = CLUSTERS.flatMapToPair(new KForwardPartitioner(1,1,2*K,SYS_PARTITIONS)).groupByKey()
	.mapValues(new LocalPattern(L,K,M,G));
//	result.cache();
	System.out.println(result.count());
    }
}

class SerialSnapshotClusterComp implements Comparator<SnapshotCluster>, Serializable {
    private static final long serialVersionUID = 2433375185377882805L;

    @Override
    public int compare(SnapshotCluster o1, SnapshotCluster o2) {
	return o1.getTS() - o2.getTS();
    }
}
