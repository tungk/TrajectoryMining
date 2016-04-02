package apriori;

import org.apache.spark.api.java.function.Function2;

import com.zaxxer.sparsebits.SparseBitSet;

/**
 * A proper reudcible function for combining edges with the same sid
 * @author a0048267
 *
 */
public class EdgeReducer implements
	Function2<SparseBitSet, SparseBitSet, SparseBitSet> {
    private static final long serialVersionUID = -522176775845102773L;
    @Override
    public SparseBitSet call(SparseBitSet v1, SparseBitSet v2) throws Exception {
	v1.or(v2);
	return v1;
    }
}
