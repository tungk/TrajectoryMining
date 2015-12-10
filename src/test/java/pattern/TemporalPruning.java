package pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TemporalPruning {
    private static int K = 5;
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

    public static ArrayList<ArrayList<Integer>> genPattern(List<Integer> input) {
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

    public static void main(String[] args) {
	List<Integer> input = Arrays
		.asList(1, 2, 3, 4, 7, 8, 9, 13,14,15,16,19,20);
	System.out.println(genPattern(input));
    }
}
