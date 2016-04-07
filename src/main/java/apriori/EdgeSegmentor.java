package apriori;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;

import model.SimpleCluster;
import model.SnapshotClusters;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * Given a cluster from DBSCAN, create pair-wise edges, each <id,id> is a key,
 * and the result is a single element bitset representing the timestamps. The
 * resulting key-value pair is <sid,tid>->bitset. We ensures that sid < tid, so
 * we do not to replicate an edge twice
 * 
 * @author a0048267
 * 
 */
public class EdgeSegmentor
	implements
	PairFlatMapFunction<SnapshotClusters, Tuple2<Integer, Integer>, IntSortedSet> {
    /**
     * 
     */
    private static final long serialVersionUID = 4125348116998762164L;

    @Override
    public Iterable<Tuple2<Tuple2<Integer, Integer>, IntSortedSet>> call(
	    SnapshotClusters t) throws Exception {
	ArrayList<Tuple2<Tuple2<Integer, Integer>, IntSortedSet>> results = new ArrayList<>();
	int my_ts = t.getTimeStamp();
	IntSortedSet current = new IntRBTreeSet();
	current.add(my_ts);
	
	for (SimpleCluster sc : t.getClusters()) {
	    // each cluster generates {n \choose 2} Integer-Integer pairs
	    IntSet objectset = sc.getObjects();
	    // change from iterable to random accessible
	    int[] cluster = objectset.toArray(new int[objectset.size()]);
	    // pair-wise join to create edge segment
	    for (int i = 0; i < cluster.length; i++) {
		for (int j = i + 1; j < cluster.length; j++) {
		    // ensures that outer is always smaller than inner
		    int outer = cluster[i];
		    int inner = cluster[j];
		    if (outer > inner) {
			int tmp = outer;
			outer = inner;
			inner = tmp;
		    }
		    Tuple2<Tuple2<Integer, Integer>, IntSortedSet> segment = new Tuple2<Tuple2<Integer, Integer>, IntSortedSet>(
			    new Tuple2<Integer, Integer>(outer, inner), current);
		    results.add(segment);
		}
	    }
	}
	return results;
    }

}
