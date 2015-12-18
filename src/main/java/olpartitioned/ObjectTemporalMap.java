package olpartitioned;

import java.util.ArrayList;
import model.Cluster;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

public class ObjectTemporalMap implements PairFlatMapFunction<ArrayList<Cluster>, Integer, Tuple2< Integer, String>> {
    private static final long serialVersionUID = -537254372850799637L;

    @Override
    public Iterable<Tuple2<Integer, Tuple2<Integer, String>>> call(
	    ArrayList<Cluster> t) throws Exception {
	ArrayList<Tuple2<Integer, Tuple2<Integer,String>>> results = new ArrayList<>();
	for(Cluster c : t) {
	    Tuple2<Integer, String> ts_cid = new Tuple2<Integer, String>(c.getTS(),c.getID());
	    for(Integer oid : c.getObjects()) {
		results.add(new Tuple2<Integer, Tuple2<Integer, String>>(oid, ts_cid));
	    }
	}
	return results;
    }
}


