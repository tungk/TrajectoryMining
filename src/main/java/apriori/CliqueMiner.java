package apriori;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import java.util.Iterator;

/**
 * Mining cliques from a star-like structure.
 * 
 * @author a0048267
 * 
 */
public class CliqueMiner
	implements
	Function<Tuple2<Integer, Iterable<Tuple2<Integer, IntSortedSet>>>, Iterable<IntSet>> {

    private static final long serialVersionUID = 714635813712741661L;

    // L and G are used for later prunings
    private final int K, M, L, G;
    private EdgeLSimplification simplifier;

    public CliqueMiner(int k, int m, int l, int g) {
	K = k;
	M = m;
	L = l;
	G = g;
	simplifier = new EdgeLSimplification(K, L, G);
    }

    @Override
    public Iterable<IntSet> call(
	    Tuple2<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> v1)
	    throws Exception {
	Iterator<Tuple2<Integer, IntSortedSet>> tmp = v1._2.iterator();
	System.out.println(v1._1);
	int count = 0;
	while (tmp.hasNext()) {
	    count++;
	    Tuple2<Integer, IntSortedSet> tuple = tmp.next();
	    // print out input for debugging purpose
	    System.out.println(tuple._1 + "\t" + tuple._2);
	}
	System.out.printf("Edges for %d is %d\n", v1._1, count);
	// time anchors to record running time
	long start, end;
	// use Apriori method for mining the cliques in bottom-up manner
	// each edge represents a 2-frequent itemset
	// building higher frequent itemset iteratively
	HashMap<IntSet, IntSortedSet> timestamp_store = new HashMap<>();
	// ArrayList<ArrayList<SparseBitSet>> candidate_set = new
	// ArrayList<>();
	ArrayList<IntSet> ground = new ArrayList<>();
	ArrayList<IntSet> output = new ArrayList<>();
	ArrayList<IntSet> candidate;
	start = System.currentTimeMillis();
	for (Tuple2<Integer, IntSortedSet> edge : v1._2) {
	    IntSet cluster = new IntOpenHashSet();
	    cluster.add(edge._1);
	    cluster.add(v1._1);
	    timestamp_store.put(cluster, edge._2);
	    ground.add(cluster);
	}
	end = System.currentTimeMillis();
	System.out.println("Initialization: " + (end - start) + " ms");
	candidate = ground;
	int level = 1;
	while (true) {
	    start = System.currentTimeMillis();
	    ArrayList<IntSet> nextLevel = new ArrayList<>();
	    HashSet<IntSet> duplicates = new HashSet<>(); // do
							  // not
							  // add
							  // duplicate
							  // objectset
							  // to
							  // the
							  // next
							  // level
	    for (int i = 0; i < candidate.size(); i++) {
		IntSet cand = candidate.get(i);
		boolean pruned = false;
		for (int j = 0; j < ground.size(); j++) {
		    IntSet grd = ground.get(j);
		    IntSet newc = new IntOpenHashSet();
		    newc.addAll(grd);
		    newc.addAll(cand);
		    if (newc.equals(cand)) {
			// a candidate should not join with its subset;
			continue;
		    }
		    // find intersections
		    IntSortedSet timestamps = new IntRBTreeSet();
		    timestamps.addAll(timestamp_store.get(grd));
		    timestamps.retainAll(timestamp_store.get(cand));

		    // trim timestamps
		    timestamps = simplifier.call(timestamps);

		    if (timestamps.size() >= K) {
			// the new candidate is significant and is potential
			if (!duplicates.contains(newc)) {
			    nextLevel.add(newc);
			    duplicates.add(newc);
			    timestamp_store.put(newc, timestamps);
			}
			// then this candidate is not qualified for
			// output
			pruned = true;
		    }
		}
		if (!pruned) {
		    IntSortedSet time_stamps = timestamp_store.get(cand);
		    if (simplifier.call(time_stamps).size() > K) {
			// time_stamp is greater than K
			if (cand.size() >= M) {
			    output.add(cand);
			}
		    }
		}
	    }
	    end = System.currentTimeMillis();
	    System.out.println("[" + v1._1 + "] Object-Grow: " + level + ", "
		    + (end - start) + " ms");
	    level++;
	    if (nextLevel.isEmpty()) {
		break;
	    } else {
		candidate = nextLevel;
	    }
	}
	System.out.println("Finished");
	return output;
    }

}
