package baseline;

import java.util.ArrayList;

import model.Cluster;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import conf.AppProperties;

public class OverlapPartitioner implements PairFlatMapFunction<ArrayList<Cluster>, Integer, ArrayList<Cluster>>{
	private static final long serialVersionUID = -8300945673433775699L;
//	private int[] schedule;
	private int start, end;
	private int l; // l is for the overlap
	private int num_of_pars, each_par_size;
	public OverlapPartitioner(int istart, int iend, int inum_pars, int ipar_size) {
	    num_of_pars = inum_pars;
	    each_par_size = ipar_size;
	    l = Integer.parseInt(AppProperties.getProperty("L"));
	    start = istart;
	    end =iend;
	}
	@Override
	public Iterable<Tuple2<Integer, ArrayList<Cluster>>> call(
		ArrayList<Cluster> t) throws Exception {
	    ArrayList<Tuple2<Integer, ArrayList<Cluster>>> results = new ArrayList<>();
	    int ts = t.get(0).getTS();
	    //find the corresponding group cluster, and emit group-cluster pair
	    int index = ts / (each_par_size - l);
	    int remain = ts - index * (each_par_size - l);
	    int prev_index = -1;
	    int next_index = -1;
	    if(remain < l) {
		if(index != 0) {
		    prev_index = index -1;    
		}
	    } else if (each_par_size - remain < l) {
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
