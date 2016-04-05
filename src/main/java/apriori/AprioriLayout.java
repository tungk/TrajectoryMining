package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;

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
    
    //the following fields are used for runlogic(), we use private field to avoid repeatedly creation
    //of objects
    private EdgeSegmentor edge_seg;
    private EdgeReducer edge_reducer;
    private EdgeFilter edge_filter;
    private EdgeMapper edge_mapper;
    private CliqueMiner clique_miner;
    
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
    }

    public void setInput(JavaRDD<SnapshotClusters> CLUSTERS) {
	input = CLUSTERS;
    }

    public JavaRDD<Iterable<IntSet>> runLogic() {
	JavaPairRDD<Tuple2<Integer, Integer>, IntSet> stage1 = input.flatMapToPair(edge_seg);
	System.out.println("Edges in stage1 " + stage1.count());
	JavaPairRDD<Tuple2<Integer, Integer>, IntSet> stage2 = stage1.reduceByKey(edge_reducer);
	System.out.println("Edges in stage2 " + stage2.count());
	JavaPairRDD<Tuple2<Integer, Integer>, IntSet> stage3 = stage2.filter(edge_filter);
	System.out.println("Totoal keys in stage3: " + stage3.count());
	JavaPairRDD<Integer, Iterable<Tuple2<Integer, IntSet>>> stage4 
		= stage3.mapToPair(edge_mapper)
		.groupByKey(1170); //39 executor, each takes 3 cores.
	
	Map<Integer, Iterable<Tuple2<Integer, IntSet>>> stage4result = stage4.collectAsMap();
	System.out.println("Stage4 Partition Result:");
	for(Map.Entry<Integer, Iterable<Tuple2<Integer, IntSet>>> entry : stage4result.entrySet()) {
	    Iterator<Tuple2<Integer,IntSet>> itr = entry.getValue().iterator();
	    int count = 0;
	    while(itr.hasNext()) {
		count++;
		itr.next();
	    }
	    System.out.println(entry.getKey() + "\t" + count);
	}
	JavaRDD<Iterable<IntSet>> stage5 = stage4.map(clique_miner);
//	JavaRDD<Iterable<SparseBitSet>> clusters = input.flatMapToPair(edge_seg)
//	.aggregateByKey(new SparseBitSet(),edge_reduce, edge_reduce)
//	.filter(edge_filter) // next we map all these edges based on their lower-end (the end with smaller IDs)
//	.mapToPair(edge_mapper)
//	.groupByKey(1950)
//	.map(clique_miner);
	return stage5;
    }

}
