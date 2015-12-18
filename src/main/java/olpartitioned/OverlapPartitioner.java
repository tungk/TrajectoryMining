package olpartitioned;

import java.util.ArrayList;

import model.Cluster;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
/**
 * The overlap partitioner, takes parameter start, end, num of partitions
 * and L, making the L-overlaped partition in the range of start-end.
 * For any input T in [start,end], find the corresponding cluster ID for this T. There possibly
 * multiple cluster ID for a single T.
 * @author a0048267
 *
 */
public class OverlapPartitioner implements PairFlatMapFunction<ArrayList<Cluster>, Integer, ArrayList<Cluster>>{
	private static final long serialVersionUID = -8300945673433775699L;
	private int L; // l is for the overlap
	private int num_of_pars, each_par_size; 
	public OverlapPartitioner(int start, int end, int inum_pars, int il) {
	    num_of_pars = inum_pars;
	    L = il;
	    each_par_size = (int) Math.ceil(((end - start + 1) + L
			* (num_of_pars - 1))
			* 1.0 / num_of_pars);
	}
	@Override
	public Iterable<Tuple2<Integer, ArrayList<Cluster>>> call(
		ArrayList<Cluster> t) throws Exception {
	    ArrayList<Tuple2<Integer, ArrayList<Cluster>>> results = new ArrayList<>();
	    int ts = t.get(0).getTS();
	    //find the corresponding group cluster, and emit group-cluster pair
	    int index = ts / (each_par_size - L);
	    int remain = ts - index * (each_par_size - L);
	    int prev_index = -1;
	    int next_index = -1;
	    if(remain < L) {
		if(index != 0) {
		    prev_index = index -1;    
		}
	    } else if (each_par_size - remain < L) {
		if(index != num_of_pars -1) {
		    next_index = index + 1;
		}
	    }
	    results.add(new Tuple2<Integer, ArrayList<Cluster>>(index, t));
	    if(prev_index != -1) {
		results.add(new Tuple2<Integer, ArrayList<Cluster>>(prev_index, t));
	    }
	    if(next_index != -1) {
		results.add(new Tuple2<Integer, ArrayList<Cluster>>(next_index, t));
	    }
	    return results;
	}
}
