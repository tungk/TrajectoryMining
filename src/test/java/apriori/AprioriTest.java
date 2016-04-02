package apriori;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.zaxxer.sparsebits.SparseBitSet;

public class AprioriTest {
    
    public static void main(String[] args) {
	final int K = 5, M = 2;
	Function<Tuple2<Integer, Iterable<Tuple2<Integer, SparseBitSet>>>, Iterable<SparseBitSet>> 
	    MineClique = new Function<Tuple2<Integer, Iterable<Tuple2<Integer, SparseBitSet>>>, Iterable<SparseBitSet>>() {
		    private static final long serialVersionUID = 2263076150006840521L;
		    @Override
		    public Iterable<SparseBitSet> call(
			    Tuple2<Integer, Iterable<Tuple2<Integer, SparseBitSet>>> v1)
			    throws Exception {
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
			    HashSet<SparseBitSet> duplicates = new HashSet<>(); //do not add duplicate objectset to the next level
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
				    SparseBitSet timestamps = SparseBitSet.and(
					    timestamp_store.get(grd),
					    timestamp_store.get(cand));
				    if (timestamps.cardinality() >= K) {
					// the new candidate is significant
					if(!duplicates.contains(newc)) {
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
		};
	
	
		
	ArrayList<Tuple2<Integer, SparseBitSet>> input_list = new ArrayList<>();
	
	SparseBitSet edge2set = new SparseBitSet();
	edge2set.set(1);
	edge2set.set(2);
	edge2set.set(3);
	edge2set.set(5);
	edge2set.set(6);
	edge2set.set(7);
	Tuple2<Integer, SparseBitSet> edge2 = 
		new Tuple2<Integer, SparseBitSet>(2, edge2set);
	input_list.add(edge2);
			
	SparseBitSet edge3set = new SparseBitSet();
	edge3set.set(1);
	edge3set.set(3);
	edge3set.set(4);
	edge3set.set(5);
	edge3set.set(6);
	Tuple2<Integer, SparseBitSet> edge3 = 
		new Tuple2<Integer, SparseBitSet>(3, edge3set);
	input_list.add(edge3);
	
	SparseBitSet edge4set = new SparseBitSet();
	edge4set.set(2);
	edge4set.set(4);
	edge4set.set(6);
	edge4set.set(7);
	edge4set.set(8);
	Tuple2<Integer, SparseBitSet> edge4 = 
		new Tuple2<Integer, SparseBitSet>(4, edge4set);
	input_list.add(edge4);
	
	
	SparseBitSet edge5set = new SparseBitSet();
	edge5set.set(1);
	edge5set.set(3);
	edge5set.set(4);
	edge5set.set(6);
	edge5set.set(7);
	edge5set.set(8);
	Tuple2<Integer, SparseBitSet> edge5 = 
		new Tuple2<Integer, SparseBitSet>(5, edge5set);
	input_list.add(edge5);
	
	
	
	Tuple2<Integer, Iterable<Tuple2<Integer, SparseBitSet>>> input = 
	new Tuple2<Integer, Iterable<Tuple2<Integer, SparseBitSet>>>(1, input_list);
	
	System.out.println(input_list);
	
	
	try {
	   Iterable<SparseBitSet> result = MineClique.call(input);
	   for(SparseBitSet r : result) {
	       System.out.println(r);
	   }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
