package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.zaxxer.sparsebits.SparseBitSet;

/**
 * Edges with cardinality less than K is not an approriate pattern.
 * In fact, edges which is not a pattern in the first place should not be of part of a
 * pattern in subsequent mining
 * @author a0048267
 *
 */
public class EdgeFilter implements Function<Tuple2<Tuple2<Integer, Integer>, IntSet>, Boolean> {
   
    private int K, L, G, M;
    public EdgeFilter(int k, int m, int l, int g) {
	K = k;
	M = m;
	L = l;
	G = g;
    }
    
    private static final long serialVersionUID = -8346260687950172856L;
    @Override
    public Boolean call(
	    Tuple2<Tuple2<Integer, Integer>, IntSet> v1)
	    throws Exception {
	return v1._2.size() >= K;
    }
}

