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
public class EagerCliqueMiner
	implements
	FlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Integer, IntSortedSet>>>, IntSet> {
    private static final long serialVersionUID = 8224251546525517682L;
    // L and G are used for later prunings
    private final int K, M, L, G;
    private EdgeLSimplification simplifier;

    public EagerCliqueMiner(int k, int m, int l, int g) {
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
	int self = v1._1;
	int count = 0;
	while (tmp.hasNext()) {
	    count++;
	    tmp.next();
//	    Tuple2<Integer, IntSortedSet> tuple = tmp.next();
	    // print out input for debugging purpose
//	    System.out.println(tuple._1 + "\t" + tuple._2);
	}
	System.out.printf("Edges for %d is %d\n", self, count);
	
	// time anchors to record running time
	long t_start, t_end;
	// use Apriori method for mining the cliques in bottom-up manner
	// each edge represents a 2-frequent itemset
	// building higher frequent itemset iteratively
	HashMap<IntSet, IntSortedSet> timestamp_store = new HashMap<>();
	ArrayList<IntSet> ground = new ArrayList<>();
	ArrayList<IntSet> output = new ArrayList<>();
	if(count < M - 1) {
	    //no need to check any more
	    System.out.println("Directly terminates");
	    return output;
	}
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
	int level = 1; // at each level, cand \in candidate will have size level
		       // +1;
		       // therefore join within cand will result an candidate
		       // with size 2 *level + 1;
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
	    // in each loop, we have two plans, if (level * 2 < M) then level =
	    // leve *2
	    // otherwise level = leve + 1;
	    //if (level * 2 <= M - 1) { if twoo high, then result in a high overhead
	    if (level < 4) { 
		// self-join wihtin candidate
		for (int i = 0, len = candidate.size(); i < len; i++) {
		    IntSet cand = candidate.get(i);
		    for (int j = i + 1; j < len; j++) {
			IntSet cand2 = candidate.get(j);
			IntSet newc = new IntOpenHashSet();
			newc.addAll(cand2);
			newc.addAll(cand);
			if (newc.size() < level * 2 + 1) { // which means
							   // generate a
							   // small set
			    continue;
			} else {
			    // find timestamps of newc
			    IntSortedSet timestamps = new IntRBTreeSet();
			    timestamps.addAll(timestamp_store.get(cand2));
			    timestamps.retainAll(timestamp_store.get(cand));
			    timestamps = simplifier.call(timestamps);
			    if (timestamps.size() >= K) {
				// the new candidate is significant and is
				// potential
				if (!duplicates.contains(newc)) {
				    nextLevel.add(newc);
				    duplicates.add(newc);
				    timestamp_store.put(newc, timestamps);
				}
			    }
			}
		    }
		}
		t_end = System.currentTimeMillis();
		level = level * 2;
		System.out.println("[" + v1._1 + "] Object-Grow to level: "
			+ level + ", " + (t_end - t_start) + " ms  cand_size:"
			+ nextLevel.size());
	    } else {
		// increment level by just 1, so cand is joined with grd
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
			    // output
			    pruned = true;
			}
		    }
		    if (!pruned) {
			IntSortedSet time_stamps = timestamp_store.get(cand);
			if (simplifier.call(time_stamps).size() >= K) {
			    // time_stamp is greater than K
			    if (cand.size() >= M) {
				output.add(cand);
			    }
			}
		    }
		}
		t_end = System.currentTimeMillis();
		level++;
		System.out.println("[" + v1._1 + "] Object-Grow to level: "
			+ level + ", " + (t_end - t_start) + " ms  cand_size:"
			+ nextLevel.size());
	    }
	    // try not to increment the level 1 by 1.
	    if (nextLevel.isEmpty()) {
		break;
	    } else {
		candidate = nextLevel;
		// an eager testing
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
			    System.out.println("Closure check directly terminates.");
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
