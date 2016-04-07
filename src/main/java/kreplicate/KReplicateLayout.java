package kreplicate;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Serializable;
import java.util.ArrayList;

import model.SnapshotClusters;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class KReplicateLayout implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -8748384975911043336L;

    private JavaRDD<SnapshotClusters> Clusters;
    private int K, L, M, G;

    public KReplicateLayout(int k, int l, int m, int g) {
	K = k;
	L = l;
	M = m;  
	G = g;
    }

    public void setInput(JavaRDD<SnapshotClusters> cLUSTERS) {
	Clusters = cLUSTERS;
    }

    public JavaPairRDD<Integer, ArrayList<IntSet>> runLogic() {
	JavaPairRDD<Integer, ArrayList<IntSet>> result = Clusters
		.flatMapToPair(new KReplicatePartitioner(2 * K + G))
		.groupByKey().mapValues(new LocalMiner(K, M, L, G));

	return result;
    }

}
