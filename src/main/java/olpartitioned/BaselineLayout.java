package olpartitioned;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import model.Cluster;
import model.GroupClusters;
import model.Pattern;
import model.SnapShot;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import util.SetCompResult;
import util.SetOps;
import util.TemporalVerification;
import conf.AppProperties;

public class BaselineLayout implements Serializable {
    private static final long serialVersionUID = -8802717808306738773L;
//    protected final JavaSparkContext jsc;
    private final int K, L, M, G, group_partition, eps, minPts;
    private final int snapshot_partitions;
    private JavaRDD<String> rawFiles;

    public BaselineLayout(JavaRDD<String> rawfiles) {
	K = Integer.parseInt(AppProperties.getProperty("K"));
	L = Integer.parseInt(AppProperties.getProperty("L"));
	M = Integer.parseInt(AppProperties.getProperty("M"));
	G = Integer.parseInt(AppProperties.getProperty("G"));
	group_partition = Integer.parseInt(AppProperties
		.getProperty("group_partition"));
	eps = Integer.parseInt(AppProperties.getProperty("eps"));
	minPts = Integer.parseInt(AppProperties.getProperty("minPts"));
	snapshot_partitions = Integer.parseInt(AppProperties
		.getProperty("snapshot_partitions"));
	rawFiles = rawfiles;
    }

//    private JavaRDD<String> readInput() {
//	return jsc.textFile(hdfs_input, hdfs_read_partitions);
//    }

    private JavaPairRDD<Integer, SnapShot> genSnapshots(JavaRDD<String> inputs) {
	return inputs.filter(new TupleFilter())
		.mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinator(), snapshot_partitions);
    }

    public void runLogic() {
//	JavaRDD<String> rawFiles = readInput();
	System.out.println("Input Ready");
	JavaPairRDD<Integer, SnapShot> snapshots = genSnapshots(rawFiles);
	System.out.println("Snapshots Ready");
	JavaRDD<ArrayList<Cluster>> clusters = snapshots.map(new DBSCANWrapper(
		eps, minPts));
	System.out.println("DBSCAN Ready");
	// then we group proximate clusters together
	// first get the max and min ts
	Tuple2<Integer, SnapShot> start = snapshots
		.max(SnapshotTSComparator.maxComp);
	Tuple2<Integer, SnapShot> end = snapshots
		.max(SnapshotTSComparator.minComp);
	JavaPairRDD<Integer, GroupClusters> groupedCluster = clusters
		.flatMapToPair(
			new OverlapPartitioner(start._1, end._1,
				group_partition, L)).groupByKey()
		.mapValues(new ClusterGrouper(8));
	System.out.println("Group Cluster Ready");
	JavaPairRDD<Integer, ArrayList<Pattern>> local_patterns = groupedCluster
		.mapValues(new LocalCMCMiner(M, L, G, K));
	System.out.println("Local Pattern Mining Ready");
	// local_patterns.collect();
	// we also need to compute the object->temporal list
	// JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>>
	// object_temporal_list =
	// clusters.flatMapToPair(new
	// ObjectTemporalMap()).groupByKey().mapValues(new TupleToList());

	// we need to generate all object patterns

	// then for each local patterns, we check whether it is global pattern
	// local_patterns.map(new PatternCheck(object_temporal_list, M, L, G,
	// K));
	// Map<Integer, ArrayList<Pattern>> global_patterns =
	// local_patterns.collectAsMap();
	// for(ArrayList<Pattern> gp : global_patterns.values()) {
	// System.out.println(gp);
	// }
	System.out.println("Join Start:");
	int join_count = 0;
	while (local_patterns.count() > 1) {
	    System.out.println("\tRound " + (join_count++));
	    local_patterns = local_patterns.mapToPair(PAIRMAP).reduceByKey(
		    PAIRREDUCE);
	}
	System.out.println("Jion Ready");
	Map<Integer, ArrayList<Pattern>> map = local_patterns.collectAsMap();
	System.out.println("Output Ready");
	for (Map.Entry<Integer, ArrayList<Pattern>> entry : map.entrySet()) {
	    System.out.print(entry.getKey() + "\t");
	    for (Pattern p : entry.getValue()) {
		System.out.print(p + "\t");
	    }
	    System.out.println();
	}
    }

    private PairFunction<Tuple2<Integer, ArrayList<Pattern>>, Integer, ArrayList<Pattern>> PAIRMAP = new PairFunction<Tuple2<Integer, ArrayList<Pattern>>, Integer, ArrayList<Pattern>>() {
	private static final long serialVersionUID = -3962959874207029419L;
	@Override
	public Tuple2<Integer, ArrayList<Pattern>> call(
		Tuple2<Integer, ArrayList<Pattern>> t) throws Exception {
	    return new Tuple2<Integer, ArrayList<Pattern>>(t._1 / 2, t._2);
	}
    };

    private Function2<ArrayList<Pattern>, ArrayList<Pattern>, ArrayList<Pattern>> PAIRREDUCE = new Function2<ArrayList<Pattern>, ArrayList<Pattern>, ArrayList<Pattern>>() {
	private static final long serialVersionUID = 3382527097155623720L;
	// merge the two local patterns
	@Override
	public ArrayList<Pattern> call(ArrayList<Pattern> v1,
		ArrayList<Pattern> v2) throws Exception {
	    ArrayList<Pattern> result = new ArrayList<>();
	    HashSet<Pattern> excluded = new HashSet<Pattern>();
	    // TODO:: further pruning exists. For example, we can prune the
	    // patterns that has is far beyond gap G
	    for (Pattern p1 : v1) {
		for (Pattern p2 : v2) {
		    if (p1.getEarlyTS() - p2.getLatestTS() > G) {
			continue;
		    }
		    if (p2.getEarlyTS() - p1.getLatestTS() > G) {
			continue; // no need to consider
		    }
		    Set<Integer> s1 = p1.getObjectSet();
		    Set<Integer> s2 = p2.getObjectSet();
		    SetCompResult scr = SetOps.setCompare(s1, s2);
		    // check common size
		    if (scr.getCommonsSize() < M) {
			// then the only pattern is p1 and p2
			continue;
		    }

		    if (scr.getStatus() == 1) {
			// p1 contains p2, so we exclude p2
			excluded.add(p2);
		    }
		    if (scr.getStatus() == 2) {
			// p2 contains p1, so we exclude p1
			excluded.add(p1);
		    }
		    if (scr.getStatus() == 3) {
			// p1 equals p2, so we exclude both
			excluded.add(p2);
			excluded.add(p1);
		    }

		    // common size greater than M
		    // create a new pattern based on the common size
		    Pattern newp = new Pattern();
		    newp.insertObjects(scr.getCommons());
		    for (Integer t : p1.getTimeSet()) {
			newp.insertTime(t);
		    }
		    for (Integer t : p2.getTimeSet()) {
			newp.insertTime(t);
		    }
		    result.add(newp);
		}
	    }
	    // for each old patterns, determine whether to further include p1 or
	    // p2
	    // this can prune many unnecessary patterns
	    for (Pattern p1 : v1) {
		if (!excluded.contains(p1)) {
		    if (TemporalVerification.isLGValidTemporal(p1.getTimeSet(),
			    L, G)) {
			result.add(p1);
		    }
		}
	    }

	    for (Pattern p2 : v2) {
		if (!excluded.contains(p2)) {
		    if (TemporalVerification.isLGValidTemporal(p2.getTimeSet(),
			    L, G)) {
			result.add(p2);
		    }
		}
	    }
	    return result;
	}
    };
}
