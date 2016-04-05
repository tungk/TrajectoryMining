package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.spark.api.java.function.Function2;

/**
 * A proper reudcible function for combining edges with the same sid
 * @author a0048267
 *
 */
public class EdgeReducer implements
	Function2<IntSet, IntSet, IntSet> {
    private static final long serialVersionUID = -522176775845102773L;
    @Override
    public IntSet call(IntSet v1, IntSet v2) throws Exception {
	v1.addAll(v2);
	return v1;
    }
}
