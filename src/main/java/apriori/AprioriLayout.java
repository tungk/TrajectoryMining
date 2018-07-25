package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import model.SnapshotClusters;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class AprioriLayout implements AlgoLayout {
    private static final long serialVersionUID = 1013697935052286484L;
    private JavaRDD<SnapshotClusters> input;
    private int K, L, M, G;
    private int clique_partitions;
    
   
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
//	clique_miner = new EagerCliqueMiner(K,M,L,G); 
	edge_simplifier = new EdgeLSimplification(K, L, G);
	clique_partitions = 195; //32 executors
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
// 	clique_miner = new EagerCliqueMiner(K,M,L,G); 
 	edge_simplifier = new EdgeLSimplification(K, L, G);
 	clique_partitions = pars;
     }
    
    @Override
    public void setInput(JavaRDD<SnapshotClusters> CLUSTERS) {
	input = CLUSTERS;
    }

    @Override
    public JavaRDD<IntSet> runLogic() {
	
	//Create edges based on the clusters at each snapshot
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage1 = input.flatMapToPair(edge_seg)
		.cache();
	System.out.println("Total edges from all snapshots:\t" + stage1.count());
	
	//Remove edges which are not candidate sequences
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage2 = stage1.reduceByKey(edge_reducer);
//		.cache();
//	System.out.println("Condensed edges in connection graph:\t"+stage2.count());
	
	JavaPairRDD<Tuple2<Integer, Integer>, IntSortedSet> stage3 = stage2
		.mapValues(edge_simplifier)
		.filter(edge_filter)
		.cache();
	System.out.println("Simplified edges:\t" + stage3.count());
	
	//Create stars for each leading vertex
	JavaPairRDD<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4 
		= stage3.mapToPair(edge_mapper)
		.groupByKey(clique_partitions);
//		.cache(); 
//	Map<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> stage4result = stage4.collectAsMap();
//	System.out.println("Star size distribution:");
//	for(Map.Entry<Integer, Iterable<Tuple2<Integer, IntSortedSet>>> entry : stage4result.entrySet()) {
//	    Iterator<Tuple2<Integer,IntSortedSet>> itr = entry.getValue().iterator();
//	    int count = 0;
//	    while(itr.hasNext()) {
//		count++;
//		itr.next();
//	    }
//	    System.out.println(entry.getKey() + "\t" + count);
//	}
	//Apriori mining for each star
	JavaRDD<IntSet> stage5 = 
		stage4.flatMap(clique_miner).cache();
	return stage5;
    }

}
