package twophasejoin;

import java.io.Serializable;
import java.util.ArrayList;

public class GenValidTemporal implements Serializable{
    private static final long serialVersionUID = -2137103284573467423L;

    public static ArrayList<ArrayList<Integer>> genValid(
	    ArrayList<Integer> input, int  L, int K, int G) {
	ArrayList<ArrayList<Integer>> results = new ArrayList<>();
	int i = 0;
	ArrayList<Integer> result = new ArrayList<>();
	int ll = 1;
	int prev = input.get(0);
	result.add(prev);
	for (i = 1; i < input.size(); i++) {
	    int current = input.get(i);
	    if (current - prev == 1) {
		result.add(current);
		ll++;
	    } else if (current - prev < G) {
		// here we need to test of L
		if (ll >= L) {
		    result.add(current);
		    ll = 1;
		} else {
		    if (result.size() >= K) {
			results.add(result);
		    }
		    result = new ArrayList<>();
		    result.add(current); 
		    ll = 1;
		}
	    } else {
		// output the current result
		if (result.size() >= K) {
		    results.add(result);
		}
		result = new ArrayList<>();
		result.add(current);
		ll = 1;
	    }
	    prev = current;
	}
	if (result.size() >= K) {
	    results.add(result);
	}
	return results;
    }
}
