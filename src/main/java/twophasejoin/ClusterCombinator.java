package twophasejoin;

import java.util.ArrayList;
import java.util.HashSet;

public class ClusterCombinator {
    public static void genCombination(ArrayList<Integer> superset, 
	    ArrayList<Integer> current, int indx,
	    int M, HashSet<ArrayList<Integer>> solution) {
	if(current.size() == M ) {
	    solution.add((ArrayList<Integer>) current.clone());
	}
	
	if(indx == superset.size()) {
	    return;
	}
	int element = superset.get(indx);
	current.add(element);
	genCombination(superset,current, indx+1, M,  solution);
	current.remove(current.size() -1);
	genCombination(superset, current, indx+1, M, solution);
    }	
}
