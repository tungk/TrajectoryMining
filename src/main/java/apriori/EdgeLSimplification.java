package apriori;

import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import org.apache.spark.api.java.function.Function;

/**
 * Use the parameter L to simplify the edge content.
 * @author a0048267
 *
 */
public class EdgeLSimplification implements Function<IntSortedSet, IntSortedSet> {
    private static final long serialVersionUID = -8205177914905770903L;

    private int K, L, G;
    public EdgeLSimplification(int k, int l, int g) {
	K = k;
	L  = l;
	G = g;
    }
    
    /**
     * it is possible to combine the L-G simplification into
     * one loop on value array, however it may introduce too much extra 
     * bookkeeping staff.
     */
    @Override	
    public IntSortedSet call(IntSortedSet v1) throws Exception {
	if(v1.size() < K) {
	    v1.clear();
	    return v1;
	}
	//cast set into arrays, since v1 is sorted, the value array is sorted automatically
	int[] value = v1.toArray(new int[v1.size()]);
	//remove the unqualified consecutive parts of timestamps
	int con_start = 0;
	for(int i = 1; i < value.length; i++) {
	    if(value[i] - value[i-1] != 1) {
		if(i - con_start < L) {
		    for(int j = con_start; j <i; j++) {
			v1.remove(value[j]);
		    }
		}
		con_start = i;
	    }
	}
	//the tail part
	if(value.length - con_start < L) {
	    for(int j = con_start; j < value.length; j++) { 
			v1.remove(value[j]);
    	    }
	}
	
	//at this moment, the edge is L-valid, we then remove the
	//L-G-L anomolies
//	System.out.println(v1);
	value = v1.toArray(new int[v1.size()]);
	int current_sum = 1;
	con_start = 0;
	for(int i = 1; i < value.length; i++) {
	    if(value[i] - value[i-1] > G) {
		if(current_sum < K) {
		    for(int j = con_start; j < i; j++) {
			v1.remove(value[j]);
		    }
		}
		con_start = i;
		current_sum = 1; 
	    } else {
		current_sum++;
	    }
	}
	
	if(value.length - con_start < K) {
	    for(int j = con_start; j < value.length; j++) { 
		v1.remove(value[j]);
	    }
	}
	return v1;
    }
    
    public static void main(String[] args) throws Exception {
	IntSortedSet r1 = new IntRBTreeSet();
	r1.addAll(Arrays.asList(1,2,3,5,6,7,8, 21,22,23,24,26,27,28,29));
	EdgeLSimplification esf = new EdgeLSimplification(4,3,3);
	System.out.println(esf.call(r1));
    }
}
