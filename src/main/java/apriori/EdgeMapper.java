package apriori;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.zaxxer.sparsebits.SparseBitSet;

/**
 * map edge based on the lower edge id, this ensures that every partiiton
 * gain enough data to discovery cliques
 * @author a0048267
 *
 */
public class EdgeMapper
	implements
	PairFunction<Tuple2<Tuple2<Integer, Integer>, SparseBitSet>, Integer, Tuple2<Integer, SparseBitSet>> {
    private static final long serialVersionUID = 8125311113760199935L;

    @Override
    public Tuple2<Integer, Tuple2<Integer, SparseBitSet>> call(
	    Tuple2<Tuple2<Integer, Integer>, SparseBitSet> t) throws Exception {
	return new Tuple2<Integer, Tuple2<Integer, SparseBitSet>>(t._1._1,
		new Tuple2<Integer, SparseBitSet>(t._1._2, t._2));
    }
}
