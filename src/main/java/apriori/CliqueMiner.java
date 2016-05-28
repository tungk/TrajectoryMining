package apriori;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.api.java.function.FlatMapFunction;
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
	FlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Integer, IntSortedSet>>>, IntSet> {

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
	    tmp.next();
	    // Tuple2<Integer, IntSortedSet> tuple = tmp.next();
	    // print out input for debugging purpose
	    // System.out.println(tuple._1 + "\t" + tuple._2);
	}
	System.out.printf("Edges for %d is %d\n", v1._1, count);
	// time anchors to record running time
	long t_start, t_end;
	// use Apriori method for mining the cliques in bottom-up manner
	// each edge represents a 2-frequent itemset
	// building higher frequent itemset iteratively
	HashMap<IntSet, IntSortedSet> timestamp_store = new HashMap<>();
	ArrayList<IntSet> ground = new ArrayList<>();
	ArrayList<IntSet> output = new ArrayList<>();
	ArrayList<IntSet> candidate;
	// initialization
	t_start = System.currentTimeMillis();
	for (Tuple2<Integer, IntSortedSet> edge : v1._2) {
	    IntSet cluster = new IntOpenHashSet();
	    cluster.add(edge._1);
	    cluster.add(v1._1);
	    timestamp_store.put(cluster, edge._2);
	    ground.add(cluster);
	}
	t_end = System.currentTimeMillis();
	System.out.println("Initialization: " + (t_end - t_start) + " ms");
	candidate = ground;
	int level = 1;
	while (true) {
	    t_start = System.currentTimeMillis();
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
		    if (cand.containsAll(grd)) {
			// a candidate should not join with its subset;
			continue;
		    }
		    IntSet newc = new IntOpenHashSet();
		    newc.addAll(grd);
		    newc.addAll(cand);
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
		    time_stamps = simplifier.call(time_stamps);
		    if (time_stamps.size() >= K) {
			// time_stamp is greater than K
			if (cand.size() >= M) {
			    output.add(cand);
			}
		    }
		}
	    }
	    t_end = System.currentTimeMillis();
	    System.out.println("[" + v1._1 + "] Object-Grow: " + level + ", "
		    + (t_end - t_start) + " ms  cand_size:" + candidate.size());
	    level++;
	    if (nextLevel.isEmpty()) {
		break;
	    } else {
		candidate = nextLevel;
		// forward closure testing
		if (candidate.size() != 0) {
		    IntSet eagerset = new IntOpenHashSet();
		    IntSortedSet eagerstamps = new IntRBTreeSet();
		    eagerset.addAll(candidate.get(0));
		    eagerstamps.addAll(timestamp_store.get(candidate.get(0)));
		    boolean early_flag = false;
		    for (int i = 1; i < candidate.size(); i++) {
			IntSet cand = candidate.get(i);
			eagerset.addAll(candidate.get(i));
			eagerstamps.retainAll(timestamp_store.get(cand));
			if (eagerstamps.size() < K) {
			    early_flag = true; // early terminates the join if
					       // timestmaps already reduces
					       // less than K
			    break;
			}
		    }
		    if (!early_flag) {
			if (eagerset.size() < M) { // no patterns with size M
						   // can be found
			    System.out
				    .println("Closure check directly terminates.");
			    break;
			}
			eagerstamps = simplifier.call(eagerstamps);
			if (eagerstamps.size() >= K) {
			    if (eagerset.size() >= M) {
				candidate.clear();
				candidate.add(eagerset);
				timestamp_store.put(eagerset, eagerstamps);
				System.out
					.printf("Closure check finished from level %d to %d\n",
						level - 1, eagerset.size() - 2);
				level = eagerset.size() - 2;
				break;
			    }
			}
		    }
		}
	    }
	}
	System.out.println("Finished");
	return output;
    }

}
