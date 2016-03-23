package kreplicate;

import java.util.ArrayList;

import model.SnapshotClusters;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class KReplicatePartitioner implements
	PairFlatMapFunction<SnapshotClusters, Integer, SnapshotClusters> {
    /**
     * 
     */
    private static final long serialVersionUID = -2946285041274070827L;

    private int P;
  
    /**
     * each cluster looks forward "p" steps;
     * @param p
     */
    public KReplicatePartitioner(int p) {
	P = p;
    }

    @Override
    public Iterable<Tuple2<Integer, SnapshotClusters>> call(
	    SnapshotClusters t) throws Exception {
	int ts = t.getTimeStamp();
	ArrayList<Tuple2<Integer, SnapshotClusters>> result =  new ArrayList<>();
	for(int i = 0; i < P; i++) {
	    result.add(new Tuple2<Integer, SnapshotClusters>(ts+i , t));
	}
	return result;
    }

}
