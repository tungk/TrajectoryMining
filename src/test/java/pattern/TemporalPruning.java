package pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.clearspring.analytics.util.Lists;

import model.TemporalCluster;
import scala.Tuple2;
import conf.AppProperties;

public class TemporalPruning {
    private static int K = 10;
    private static int L = 2;
    private static int G = 3;

    public static Boolean call(List<Integer> a) {
	boolean valid = true;
	int num_of_ts = 0;
	int consecutive = 0;
	int current = 0;
	int next = 0;
	Iterator<Integer> itr = a.iterator();
	if (itr.hasNext()) {
	    current = itr.next();
	    consecutive = 1;
	    num_of_ts++;
	}
	while (itr.hasNext()) {
	    next = itr.next();
	    num_of_ts++;
	    if (next == current + 1) {
		consecutive++;
	    } else {
		// check gap
		if (next - current > G) {
		    valid = false;
		    break;
		}
		// check prev length
		if (consecutive < L) {
		    valid = false;
		    break;
		}
		consecutive = 1;
	    }
	    current = next;
	}
	if (num_of_ts < K) {
	    valid = false;
	}
	return valid;
    }

    public static void main(String[] args) {
	List<Integer> input = Arrays.asList(1,2,5,6,7,8,9,10,11);
	System.out.println(call(input));
    }
}
