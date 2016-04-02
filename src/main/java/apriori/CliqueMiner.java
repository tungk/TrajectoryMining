package apriori;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import java.util.Iterator;

import com.zaxxer.sparsebits.SparseBitSet;

public class CliqueMiner
	implements
	Function<Tuple2<Integer, Iterable<Tuple2<Integer, SparseBitSet>>>, Iterable<SparseBitSet>> {
    /**
     * 
     */
    private static final long serialVersionUID = 714635813712741661L;
    
    //L and G are used for later prunings
    private final int K, M, L, G;

    public CliqueMiner(int k, int m, int l, int g) {
	K = k;
	M = m;
	L = l;
	G = g;
    }

    @Override
    public Iterable<SparseBitSet> call(
	    Tuple2<Integer, Iterable<Tuple2<Integer, SparseBitSet>>> v1)
	    throws Exception {
	
	Iterator<Tuple2<Integer, SparseBitSet>> tmp = v1._2.iterator();
	int count = 0;
	while(tmp.hasNext()) {
	    count++;
	    tmp.next();
	}
	System.out.printf("Edges for %d is %d\n", v1._1, count);
	// use Apriori method for mining the cliques in bottom-up manner
	// each edge represents a 2-frequent itemset
	// building higher frequent itemset iteratively
	HashMap<SparseBitSet, SparseBitSet> timestamp_store = new HashMap<>();
	// ArrayList<ArrayList<SparseBitSet>> candidate_set = new
	// ArrayList<>();
	ArrayList<SparseBitSet> ground = new ArrayList<>();
	ArrayList<SparseBitSet> output = new ArrayList<>();
	ArrayList<SparseBitSet> candidate;

	for (Tuple2<Integer, SparseBitSet> edge : v1._2) {
	    SparseBitSet cluster = new SparseBitSet();
	    cluster.set(edge._1);
	    cluster.set(v1._1);
	    timestamp_store.put(cluster, edge._2);
	    ground.add(cluster);
	}
	candidate = ground;
	while (true) {
	    ArrayList<SparseBitSet> nextLevel = new ArrayList<>();
	    HashSet<SparseBitSet> duplicates = new HashSet<>(); // do
								// not
								// add
								// duplicate
								// objectset
								// to
								// the
								// next
								// level
	    for (int i = 0; i < candidate.size(); i++) {
		SparseBitSet cand = candidate.get(i);
		boolean pruned = false;
		for (int j = 0; j < ground.size(); j++) {
		    SparseBitSet grd = ground.get(j);
		    SparseBitSet newc = SparseBitSet.or(grd, cand);
		    if (newc.equals(cand)) {
			// a candidate should not join with its subset;
			continue;
		    }
		    SparseBitSet timestamps = SparseBitSet
			    .and(timestamp_store.get(grd),
				    timestamp_store.get(cand));
		    if (timestamps.cardinality() >= K) {
			// the new candidate is significant
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
		    if (cand.cardinality() >= M) {
			output.add(cand);
		    }
		}
	    }
	    if (nextLevel.isEmpty()) {
		break;
	    } else {
		candidate = nextLevel;
	    }
	}
	return output;
    }

}
