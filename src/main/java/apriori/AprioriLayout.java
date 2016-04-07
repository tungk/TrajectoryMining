package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.io.Serializable;
import java.util.Map;

import model.SnapshotClusters;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;
import java.util.Iterator;


public class AprioriLayout implements Serializable {
    private static final long serialVersionUID = 1013697935052286484L;
    private JavaRDD<SnapshotClusters> input;
    private int K, L, M, G;
    private int clique_partitions;
    
    //the following fields are used for runlogic(), we use private field to avoid repeatedly creation
    //of objects
    private EdgeSegmentor edge_seg;
    private EdgeReducer edge_reducer;
    private EdgeFilter edge_filter;
    private EdgeMapper edge_mapper;
    private CliqueMiner clique_miner;
    private EdgeLSimplification edge_simplifier;
    
    
    public AprioriLayout(int k, int m, int l, int g) {
	K = k; 
	L = l;
	M = m;
	G = g;
	edge_seg = new EdgeSegmentor();
	edge_reducer = new EdgeReducer();
	edge_filter = new EdgeFilter(K,M,L,G);
	edge_mapper = new EdgeMapper();
	clique_miner = new CliqueMiner(K,M,L,G);
	edge_simplifier = new EdgeLSimplification(K, L, G);
	clique_partitions = 1170; //39 executor, each takes 3 cores, each core execute 10 tasks
    }
    
    public AprioriLayout(int k, int m, int l, int g, int pars) {
 	K = k; 
 	L = l;
 	M = m;
 	G = g;
 	edge_seg = new EdgeSegmentor();
 	edge_reducer = new EdgeReducer();
 	edge_filter = new EdgeFilter(K,M,L,G);
 	edge_mapper = new EdgeMapper();
 	clique_miner = new CliqueMiner(K,M,L,G);
 	edge_simplifier = new EdgeLSimplification(K, L, G);
 	clique_partitions = pars;
     }
    

    public void setInput(JavaRDD<SnapshotClusters> CLUSTERS) {
	input = CLUSTERS;
    }

    public JavaRDD<Iterable<IntSet>> runLogic() {
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage1 = input.flatMapToPair(edge_seg);
	System.out.println("Edges in stage1 " + stage1.count());
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage2 = stage1.reduceByKey(edge_reducer);
	System.out.println("Edges in stage2 " + stage2.count());
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage3 = stage2.mapValues(edge_simplifier)
		.filter(edge_filter);
	System.out.println("Totoal keys in stage3: " + stage3.count());
	JavaPairRDD<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4 
		= stage3.mapToPair(edge_mapper)
		.groupByKey(clique_partitions); 
	
	Map<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4result = stage4.collectAsMap();
	System.out.println("Stage4 Partition Result:");
	for(Map.Entry<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> entry : stage4result.entrySet()) {
	    Iterator<Tuple2<Integer,IntSortedSet>> itr = entry.getValue().iterator();
	    int count = 0;
	    while(itr.hasNext()) {
		count++;
		itr.next();
	    }
	    System.out.println(entry.getKey() + "\t" + count);
	}
	JavaRDD<Iterable<IntSet>> stage5 = stage4.map(clique_miner);
	return stage5;
    }

}
