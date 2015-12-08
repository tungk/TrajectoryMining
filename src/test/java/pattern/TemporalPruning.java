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
    private static int K = 8;
    private static int L = 4;
    private static int G = 2;

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

    public static ArrayList<ArrayList<Integer>> genPattern(List<Integer> input) {
	ArrayList<ArrayList<Integer>> result = new ArrayList<>();
	int p_start = 0;
	int p_prev = 0;
	int p_consecutive = 1;
	for (int p_next = 1; p_next < input.size(); p_next++) {
	    int delta = input.get(p_next) - input.get(p_prev);
	    System.out.println(p_next + "\t" + delta+"\t"+p_start+"\t"+p_prev+"\t"+p_next+"\t"+p_consecutive );
	    if (delta == 1) {
		p_prev = p_next;
		p_consecutive++;
		continue;
	    }
	    // checking gaps 
	    if (delta <= G) {
		// check consecutive
		if (p_consecutive < L) {
		    // do nothing;
		    p_start = p_next;
		    p_consecutive = 1;
		} else {
		    // continue to extend
		}
		p_prev = p_next;
		continue;
	    } else {
		// checking whether to output
		if (p_consecutive < L) {
		    // do nothing;
		    p_start = p_next;
		} else {
		    ArrayList<Integer> ps = new ArrayList<>();
		    for (int i = p_start; i <= p_prev; i++) {
			ps.add(input.get(i));
		    }
		    result.add(ps);
		}
		p_consecutive = 1;
		p_prev = p_next;
		p_start = p_next;
		continue;
	    }
	}
	return result;
    }

    public static void main(String[] args) {
	List<Integer> input = Arrays
		.asList(1, 2, 3, 4, 7, 8, 9, 10, 12, 13, 15);
	// System.out.println(call(input));
	System.out.println(genPattern(input));
    }
}
