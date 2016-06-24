package apriori;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import model.SnapshotClusters;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import common.SerializableComparator;

import scala.Tuple2;

public class AprioriWithLB implements AlgoLayout {
    private static final long serialVersionUID = -5333188031870672749L;
    private JavaRDD<SnapshotClusters> input;
    private int K, L, M, G;
    private int clique_partitions;

    // the following fields are used for runlogic(), we use private field to
    // avoid repeatedly creation
    // of objects
    private EdgeSegmentor edge_seg;
    private EdgeReducer edge_reducer;
    private EdgeFilter edge_filter;
    private EdgeMapper edge_mapper;
    private CliqueMiner clique_miner;
//    private EagerCliqueMiner clique_miner;
    private EdgeLSimplification edge_simplifier;

    public AprioriWithLB(int k, int m, int l, int g, int pars) {
	K = k;
	L = l;
	M = m;
	G = g;
	edge_seg = new EdgeSegmentor();
	edge_reducer = new EdgeReducer();
	edge_filter = new EdgeFilter(K, M, L, G);
	edge_mapper = new EdgeMapper();
	 clique_miner = new CliqueMiner(K,M,L,G);
//	clique_miner = new EagerCliqueMiner(K, M, L, G);
	edge_simplifier = new EdgeLSimplification(K, L, G);
	clique_partitions = pars;
    }

    @Override
    public void setInput(JavaRDD<SnapshotClusters> CLUSTERS) {
	this.input = CLUSTERS;
    }

    @Override
    public JavaRDD<IntSet> runLogic() {
	// Create edges based on the clusters at each snapshot
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage1 = input
		.flatMapToPair(edge_seg).cache();
	System.out
		.println("Total edges from all snapshots:\t" + stage1.count());

	// Remove edges which are not candidate sequences
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage2 = stage1
		.reduceByKey(edge_reducer).cache();
	System.out.println("Condensed edges in connection graph:\t"
		+ stage2.count());

	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage3 = stage2
		.mapValues(edge_simplifier).filter(edge_filter).cache();
	System.out.println("Simplified edges:\t" + stage3.count());
	// this point diverses from AprioriLayout

	Map<Tuple2<Integer, Integer>, IntSortedSet> exact = stage3.collectAsMap();
	long time_start = System.currentTimeMillis();
	HashMap<Integer, Integer> stats = new HashMap<>();
	for (Tuple2<Integer, Integer> key : exact.keySet()) {
	    if (!stats.containsKey(key._1)) {
		stats.put(key._1, 0);
	    }
	    stats.put(key._1, stats.get(key._1) + 1);
	}
	List<Entry<Integer, Integer>> list = new LinkedList<Entry<Integer, Integer>>(stats.entrySet());
	Collections.sort(list, new SerializableComparator<Entry<Integer,Integer>>(){
	    private static final long serialVersionUID = 1121563559166999659L;
	    @Override
	    public int compare(Entry<Integer, Integer> o1,
		    Entry<Integer, Integer> o2) {
		return o2.getValue() - o1.getValue();
	    }
	});
	// analyze the stats to find the id-bucket mapping, round robin first;
	int pointer = 0;
	int[] bucket_weights = new int[clique_partitions];
	HashMap<Integer, Integer> id_bucket_mapping = new HashMap<>();
	for (Entry<Integer,Integer> entry : list) {
	    int min = bucket_weights[0];
	    pointer = 0;
	    for(int i = 1; i < bucket_weights.length; i++) {
		if(bucket_weights[i] < min) {
		    min = bucket_weights[i];
		    pointer = i;
		}
	    }
	    id_bucket_mapping.put(entry.getKey(), pointer);
	    bucket_weights[pointer] += entry.getValue();
	}
	long time_end = System.currentTimeMillis();
	System.out.println("[LOAD SCHEDULE]: " + (time_end - time_start) + " ms");

	JavaPairRDD<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4 = stage3
		.mapToPair(edge_mapper)
		.groupByKey(new TempPartitioner(clique_partitions, id_bucket_mapping))
		.cache();
//	Map<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4result = stage4
//		.collectAsMap();
//	System.out.println("Star size distribution:");
//	for (Map.Entry<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> entry : stage4result
//		.entrySet()) {
//	    Iterator<Tuple2<Integer, IntSortedSet>> itr = entry.getValue()
//		    .iterator();
//	    int count = 0;
//	    while (itr.hasNext()) {
//		count++;
//		itr.next();
//	    }
//	    System.out.println(entry.getKey() + "\t" + count);
//	}

	// Apriori mining for each star
	JavaRDD<IntSet> stage5 = stage4.flatMap(clique_miner).cache();
	return stage5;
    }

}
