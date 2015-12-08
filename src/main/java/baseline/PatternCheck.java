package baseline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import model.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class PatternCheck implements
	Function<Tuple2<Integer, ArrayList<Pattern>>, ArrayList<Pattern>> {
    private static final long serialVersionUID = -7942827644871299641L;
    private final JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> object_temporal_list;
    private int M, L, G, K;
    public PatternCheck(
	    JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> inputlist, int m, int l, int g, int k) {
	this.object_temporal_list = inputlist;
	M = m;
	L = l;
	G = g;
	K = k;
    }

    @Override
    public ArrayList<Pattern> call(Tuple2<Integer, ArrayList<Pattern>> v1)
	    throws Exception {
	ArrayList<Pattern> local_pattern = v1._2;
	ArrayList<Pattern> result = new ArrayList<>();
	for (Pattern p : local_pattern) {
	    // check whether the object list forms a valid pattern
	    Pattern full_p = formpattern(p);
	    if (p != null) {
		result.add(p);
	    }
	}
	return result;
    }

    private Pattern formpattern(Pattern p) {
	TreeSet<Tuple2<Integer, String>> temporals = new TreeSet<Tuple2<Integer, String>>();
	Set<Integer> objs = p.getObjectSet();
	// we create the temporal list of those objs
	object_temporal_list.aggregate(temporals, new TreeSetSeqOp(objs),
		combOp);
	//now we check the temporal to see whether the pattern is fullfill
	//group temporals based on its cluster ids;
	HashMap<String, ArrayList<Integer>> patterns = new HashMap<>();
	for(Tuple2<Integer,String>  tpl : temporals) {
	    if(!patterns.containsKey(tpl._2)){
		patterns.put(tpl._2, new ArrayList<Integer>());
	    }
	    patterns.get(tpl._2).add(tpl._1);
	}
	
	ArrayList<Pattern> results = new ArrayList<>();
	for(ArrayList<Integer> tps : patterns.values()) {
	    if(tps.size() >= K) {
		int pos = 1;
		int p_start = 0;
		int prev = 0;
		Collections.sort(tps);
		for(; pos < tps.size(); pos++) {
		    if(tps.get(pos) - tps.get(prev) != 1) {
			//check gap constraint
			int delta = tps.get(pos) - tps.get(prev);
			if(delta  <= G) {
			    //we can continue to extend;
			} else {
			    //check the range p_start, prev
			    
			}
		    }
		}
	    }
	}
	
	return null;
    }

    private Function2<TreeSet<Tuple2<Integer, String>>, TreeSet<Tuple2<Integer, String>>, TreeSet<Tuple2<Integer, String>>> combOp = new Function2<TreeSet<Tuple2<Integer, String>>, TreeSet<Tuple2<Integer, String>>, TreeSet<Tuple2<Integer, String>>>() {
	private static final long serialVersionUID = -371201587438434626L;

	// combines to tree set into one
	@Override
	public TreeSet<Tuple2<Integer, String>> call(
		TreeSet<Tuple2<Integer, String>> v1,
		TreeSet<Tuple2<Integer, String>> v2) throws Exception {
	    v1.addAll(v2);
	    return v1;
	}
    };
}
