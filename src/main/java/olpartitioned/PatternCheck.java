package olpartitioned;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	    ArrayList<Pattern> full_p = formpattern(p);
	    if(!full_p.isEmpty()) {
		result.addAll(full_p);
	    }
	}
	return result;
    }

    private ArrayList<Pattern> formpattern(Pattern local_p) {
	TreeSet<Tuple2<Integer, String>> temporals = new TreeSet<Tuple2<Integer, String>>();
	Set<Integer> objs = local_p.getObjectSet();
	if(objs.size() < M) {
	    return new ArrayList<Pattern>();
	}
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
	    ArrayList<ArrayList<Integer>> tpattns = genPattern(tps); 
	    for(ArrayList<Integer> tp : tpattns) {
		Pattern pp = new Pattern();
		pp.insertPattern(objs, tp);
		results.add(pp);
	    }
	}
	return results;
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

    public ArrayList<ArrayList<Integer>> genPattern(List<Integer> input) {
	ArrayList<ArrayList<Integer>> result = new ArrayList<>();
	ArrayList<ArrayList<Integer>> result2 = new ArrayList<>();
	int last = 0;
	ArrayList<Integer> current = new ArrayList<>();
	current.add(input.get(last));
	for (int p = 1; p < input.size(); p++) {
	    if (input.get(p) - input.get(last) == 1) {
		current.add(input.get(p));
	    } else {
		if (current.size() >= L) {
		    result.add(current);
		}
		current = new ArrayList<>();
		current.add(input.get(p));
	    }
	    last = p;
	}
	if (current.size() >= L) {
	    result.add(current);
	}
	//at this moment, result only contains the patterns with L-compatible
	//merge patterns with g-constraint
	for(int i = result.size() - 1; i>=1; i--) {
	    ArrayList<Integer> lst = result.get(i);
	    ArrayList<Integer> prev = result.get(i-1);
	    if(lst.get(0) - prev.get(prev.size() -1) <=G) {
		//merge these two;
		for(Integer ts : lst) {
		    prev.add(ts);
		}
	    } else {
		//check prev's validity
		if(lst.size() >= K) {
		    result2.add(lst);
		}
	    }
	    result.remove(i);
	}
	for(ArrayList<Integer> pat : result) {
	    if(pat.size() >= K) {
		result2.add(pat);
	    }
	}
	return result2;
    }
}
