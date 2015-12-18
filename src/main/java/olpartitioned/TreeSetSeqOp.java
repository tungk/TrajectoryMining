package olpartitioned;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class TreeSetSeqOp implements Function2<TreeSet<Tuple2<Integer,String>>, Tuple2<Integer, ArrayList<Tuple2<Integer, String>>>, TreeSet<Tuple2<Integer,String>>> {
    private static final long serialVersionUID = -1532300063496043812L;
    private final TreeSet<Integer> valid_objs;
    public TreeSetSeqOp(Set<Integer> objs) {
	valid_objs = (TreeSet<Integer>) objs;
    }
    
    @Override
    public TreeSet<Tuple2<Integer,String>> call(TreeSet<Tuple2<Integer,String>> v1,
	    Tuple2<Integer, ArrayList<Tuple2<Integer, String>>> v2)
	    throws Exception {
	if(valid_objs.contains(v2._1)) {
	    for(Tuple2<Integer,String> tpl : v2._2){
		v1.add(tpl);
	    }
	}
	return v1;
    }

}
