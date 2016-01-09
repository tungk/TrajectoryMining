package twophasejoin;

import java.util.ArrayList;
import java.util.List;

import model.Cluster;
import model.SnapShot;
import olpartitioned.DBSCANWrapper;
import olpartitioned.SnapshotCombinator;
import olpartitioned.SnapshotGenerator;
import olpartitioned.TupleFilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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

	// we collect the set of TS's
	List<Integer> TS_SET = TS_CLUSTERS.map(COUNT_KEYS).collect();

	// DBSCAN
	JavaRDD<ArrayList<Cluster>> CLUSTERS = TS_CLUSTERS
		.map(new DBSCANWrapper(conf.Constants.EPS,
			conf.Constants.MINPTS));

	CLUSTERS.mapToPair(SPLIT_PAIR)
		.flatMapToPair(new SnapShotScatter(TS_SET)).groupByKey()
		.mapValues(new CMCMethod(k, l, m, g));

    }
}
