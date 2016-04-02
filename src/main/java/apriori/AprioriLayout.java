package apriori;

import java.io.Serializable;
import model.SnapshotClusters;

import org.apache.spark.api.java.JavaRDD;
import com.zaxxer.sparsebits.SparseBitSet;


public class AprioriLayout implements Serializable {
    private static final long serialVersionUID = 1013697935052286484L;
    private JavaRDD<SnapshotClusters> input;
    private static int K, L, M, G;
    
    //the following fields are used for unlogic(), we use static fields to 
    //avoid repeatedly creating objects
    private static EdgeSegmentor edge_seg = new EdgeSegmentor();
    private static EdgeReducer edge_reduce = new EdgeReducer();
    private static EdgeFilter edge_filter = new EdgeFilter(K,M,L,G);
    private static EdgeMapper edge_mapper = new EdgeMapper();
    private static CliqueMiner clique_miner = new CliqueMiner(K,M,L,G);
    
    public AprioriLayout(int k, int m, int l, int g) {
	K = k;
	L = l;
	M = m;
	G = g;
    }

    public void setInput(JavaRDD<SnapshotClusters> CLUSTERS) {
	input = CLUSTERS;
    }

    public JavaRDD<Iterable<SparseBitSet>> runLogic() {
	JavaRDD<Iterable<SparseBitSet>> clusters = input.flatMapToPair(edge_seg)
	.aggregateByKey(new SparseBitSet(),edge_reduce, edge_reduce)
	.filter(edge_filter) // next we map all these edges based on their lower-end (the end with smaller IDs)
	.mapToPair(edge_mapper)
	.groupByKey()
	.map(clique_miner);
	return clusters;
    }

}
