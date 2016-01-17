package pattern;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
public class CombinationGenerator <T> {
    @SuppressWarnings("unchecked")
    public void genCombination(ArrayList<T> superset, ArrayList<T> current, int indx,
	    int M, HashSet<ArrayList<T>> solution) {
	if(current.size() == M ) {
	    solution.add((ArrayList<T>) current.clone());
	}
	
	if(indx == superset.size()) {
	    return;
	}
	T element = superset.get(indx);
	current.add(element);
	genCombination(superset,current, indx+1, M,  solution);
	current.remove(current.size() -1);
	genCombination(superset, current, indx+1, M, solution);
    }	
    
    public static void main(String[] args) {
	ArrayList<Integer> superset =
		new ArrayList<>(Arrays.asList(1,2,3,4,5));
	CombinationGenerator<Integer> cg = new CombinationGenerator<>();
	HashSet<ArrayList<Integer>> solutions = new HashSet<>();
	cg.genCombination(superset, new ArrayList<Integer>(), 0, 3, solutions);
	System.out.println(solutions);
    } 
}
