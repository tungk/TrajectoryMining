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

    private int eta;
    public KReplicateLayout(int k, int l, int m, int g) {
	K = k;
	L = l;
	M = m;  
	G = g;
	eta =  ((int) (Math.ceil(k*1.0/l)) - 1 ) *(g-1) + K+L-1;
    }

    public void setInput(JavaRDD<SnapshotClusters> cLUSTERS) {
	Clusters = cLUSTERS;
    }

    public JavaPairRDD<Integer, ArrayList<IntSet>> runLogic() {
	JavaPairRDD<Integer, ArrayList<IntSet>> result = Clusters
		.flatMapToPair(new KReplicatePartitioner(eta))
		.groupByKey().mapValues(new LocalMiner(K, M, L, G));

	return result;
    }

}
