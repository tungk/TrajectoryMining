package twophasejoin;

import java.util.ArrayList;
import java.util.List;

import model.Cluster;
import model.SnapShot;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * determines which keys does a snapshot goes to
 * @author a0048267
 *
 */
public class SnapShotScatter implements PairFlatMapFunction<Tuple2<Integer, ArrayList<Cluster>>, Integer, ArrayList<Cluster>> {
    private static final long serialVersionUID = -3033043257133699002L;
    
    private ArrayList<Integer> ids;
    public SnapShotScatter(List<Integer> input_ids) {
	ids = new ArrayList<Integer>(input_ids);
    }
    
    @Override
    public Iterable<Tuple2<Integer, ArrayList<Cluster>>> call(Tuple2<Integer, ArrayList<Cluster>> v1)
	    throws Exception {
	ArrayList<Tuple2<Integer,ArrayList<Cluster>>> result = new ArrayList<>();
	for(Integer id:ids) {
	    result.add(new Tuple2<Integer, ArrayList<Cluster>>(id, v1._2));
	}
	return result;
    }
    
}
