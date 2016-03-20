package kreplicate;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class KReplicatePartitioner implements
	PairFlatMapFunction<ArrayList<SimpleCluster>, Integer, ArrayList<SimpleCluster>> {
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
    public Iterable<Tuple2<Integer, ArrayList<SimpleCluster>>> call(
	    ArrayList<SimpleCluster> t) throws Exception {
	int ts = t.get(0).getTS();
	ArrayList<Tuple2<Integer, ArrayList<SimpleCluster>>> result =  new ArrayList<>();
	for(int i = 0; i < P; i++) {
	    result.add(new Tuple2<Integer, ArrayList<SimpleCluster>>(ts+i , t));
	}
	return result;
    }

}
