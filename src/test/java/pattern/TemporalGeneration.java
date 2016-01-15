package pattern;

import java.util.ArrayList;
import java.util.Arrays;

public class TemporalGeneration {
    private static int L = 4;
    private static int G = 3;
    private static int K = 4;

    public static ArrayList<ArrayList<Integer>> genValid(
	    ArrayList<Integer> input) {
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

    public static void main(String[] args) {
	ArrayList<Integer> input = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 6,
		7, 8, 9, 10, 15, 16, 17, 18, 23, 24, 25));
	System.out.println(genValid(input));
	
	ArrayList<Integer> input2 = new ArrayList<>(Arrays.asList(
		1, 2, 3, 4,
		6, 7, 8, 9, 10, 
		15, 16, 17, 18, 
		23, 24, 
		30, 31, 32, 33, 34, 
		36, 37,38,39));
	System.out.println(genValid(input2));
    }
}
