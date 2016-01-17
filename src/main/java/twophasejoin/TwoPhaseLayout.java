package twophasejoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import model.Cluster;
import model.SnapShot;
import olpartitioned.DBSCANWrapper;
import olpartitioned.SnapshotCombinator;
import olpartitioned.SnapshotGenerator;
import olpartitioned.TupleFilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import conf.AppProperties;

import scala.Tuple2;

public class TwoPhaseLayout {
    private JavaRDD<String> inputs;
    private int k, l, m, g;
    private Function<Tuple2<Integer, SnapShot>, Integer> COUNT_KEYS = new Function<Tuple2<Integer, SnapShot>, Integer>() {
	private static final long serialVersionUID = 7728952857159555992L;

	@Override
	public Integer call(Tuple2<Integer, SnapShot> v1) throws Exception {
	    return v1._1;
	}

    };
    private PairFunction<ArrayList<Cluster>, Integer, ArrayList<Cluster>> SPLIT_PAIR = new PairFunction<ArrayList<Cluster>, Integer, ArrayList<Cluster>>() {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5501169985764826464L;

	@Override
	public Tuple2<Integer, ArrayList<Cluster>> call(ArrayList<Cluster> t)
		throws Exception {
	    int key = t.get(0).getTS();
	    return new Tuple2<Integer, ArrayList<Cluster>>(key, t);
	}
    };

    public TwoPhaseLayout(JavaRDD<String> textFile) {
	inputs = textFile;
	k = Integer.parseInt(AppProperties.getProperty("K"));
	l = Integer.parseInt(AppProperties.getProperty("L"));
	m = Integer.parseInt(AppProperties.getProperty("M"));
	g = Integer.parseInt(AppProperties.getProperty("G"));
    }

    /**
     * Use a two phase logic to
     */
    public void runLogic() {
	// DBSCAN first
	JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = inputs
		.filter(new TupleFilter())
		.mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinator(),
			conf.Constants.SNAPSHOT_PARTITIONS);
	// DBSCAN
	JavaRDD<ArrayList<Cluster>> CLUSTERS = TS_CLUSTERS.map(
		new DBSCANWrapper(conf.Constants.EPS, conf.Constants.MINPTS))
		.filter(new Function<ArrayList<Cluster>, Boolean>() {
		    private static final long serialVersionUID = -7032753424628524015L;

		    @Override
		    public Boolean call(ArrayList<Cluster> v1) throws Exception {
			return v1.size() > m;
		    }
		});
	// we then use G-overlapped partition to filter clusters
	Integer max = TS_CLUSTERS
		.max(new Comparator<Tuple2<Integer, SnapShot>>() {
		    @Override
		    public int compare(Tuple2<Integer, SnapShot> o1,
			    Tuple2<Integer, SnapShot> o2) {
			return o1._1 - o2._1;
		    }
		})._1;

	Integer min = TS_CLUSTERS
		.min(new Comparator<Tuple2<Integer, SnapShot>>() {
		    @Override
		    public int compare(Tuple2<Integer, SnapShot> o1,
			    Tuple2<Integer, SnapShot> o2) {
			return o1._1 - o2._1;
		    }
		})._1;

	Function<Iterable<ArrayList<Cluster>>, ArrayList<Cluster>> tt;
	CLUSTERS.flatMapToPair(new OverlapPartitioner(min, max, 31, g))
		.groupByKey()
		.mapValues(new CMCFilter(m, g))
		.flatMapToPair(
			new PairFlatMapFunction<Tuple2<Integer, ArrayList<Cluster>>, Cluster, Integer>() {
			    private static final long serialVersionUID = -3424090995370901210L;

			    @Override
			    public Iterable<Tuple2<Cluster, Integer>> call(
				    Tuple2<Integer, ArrayList<Cluster>> t)
				    throws Exception {
				ArrayList<Tuple2<Cluster, Integer>> result = new ArrayList<>();
				for (Cluster c : t._2) {
				    HashSet<ArrayList<Integer>> combinations = new HashSet<>();
				    ClusterCombinator.genCombination(new ArrayList<Integer>(c.getObjects()), new ArrayList<Integer>(), 0, m, combinations);
				    for(ArrayList<Integer> combination : combinations) {
					Cluster c2 = new Cluster(c.getContext());
					c2.addAllObjects(combination);
					 result.add(new Tuple2<Cluster, Integer>(c2,
						    c.getTS()));
				    }
				}
				return result;
			    }
			})
		.groupByKey()
		.mapValues(
			new Function<Iterable<Integer>, ArrayList<ArrayList<Integer>>>() {
			    private static final long serialVersionUID = -7245979451658206130L;
			    @Override
			    public ArrayList<ArrayList<Integer>> call(
				    Iterable<Integer> v1) throws Exception {
				ArrayList<Integer> input = new ArrayList<>();
				// add v1 to input set;
				for (Integer i : v1) {
				    input.add(i);
				}
				// sort input
				Collections.sort(input);
				return GenValidTemporal
					.genValid(input, l, k, g);
			    }
			}); // no need to filter on the empty ones;
    }
}
