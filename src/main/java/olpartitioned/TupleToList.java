package olpartitioned;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * input is the iterable integer string pair, output is a list of those values
 * we sort the list by Integer field which is the time sequence
 * @author a0048267
 *
 */
public class TupleToList
	implements
	Function<Iterable<Tuple2<Integer, String>>, ArrayList<Tuple2<Integer, String>>> {
    private static final long serialVersionUID = -448356866928714812L;

    @Override
    public ArrayList<Tuple2<Integer, String>> call(
	    Iterable<Tuple2<Integer, String>> v1) throws Exception {
	ArrayList<Tuple2<Integer,String>> result = new ArrayList<>();
	for(Tuple2<Integer,String> tuple : v1) {
	    result.add(tuple);
	}
	Collections.sort(result, new Comparator<Tuple2<Integer,String>>() {
	    @Override
	    public int compare(Tuple2<Integer, String> o1,
		    Tuple2<Integer, String> o2) {
		return o1._1 - o2._1;
	    }
	});
	return null;
    }
}